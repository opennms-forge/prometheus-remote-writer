/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-reader over a WAL directory. Tracks a monotonic read offset in
 * the WAL's global address space and drains payloads in batches via
 * {@link #nextBatch(int)}. Cross-segment reads are transparent: when the
 * current segment is exhausted, the reader closes it and opens the next
 * segment whose {@code startOffset} matches.
 *
 * <p>The reader uses {@link WalSegment#openForRead} so it has its own
 * {@link java.nio.channels.FileChannel} per segment — independent from
 * any {@link WalWriter} that's concurrently appending. Each
 * {@code nextBatch} re-queries {@code channel.size()} so bytes
 * committed by the writer after the previous batch are picked up on the
 * next call.
 *
 * <p>Thread-safety: single-reader discipline. Not safe to invoke
 * {@link #nextBatch} or {@link #advanceTo} concurrently from multiple
 * threads. Concurrent use with a {@link WalWriter} is safe.
 */
public final class WalReader implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(WalReader.class);

    private final Path dir;
    private final int maxPayload;

    private long currentOffset;
    private WalSegment currentSegment; // null until the first nextBatch call
    private boolean closed;

    public WalReader(Path dir, long initialOffset, int maxPayload) {
        this.dir = dir;
        this.currentOffset = initialOffset;
        this.maxPayload = maxPayload;
    }

    /** Global offset of the next unread frame. */
    public long currentOffset() { return currentOffset; }

    /**
     * Drain up to {@code maxSamples} payloads from the current offset
     * forward. Cross-segment reads are transparent. Returns an empty
     * {@link ReadResult} (payloads.isEmpty()) when the reader is caught
     * up to the writer.
     */
    public ReadResult nextBatch(int maxSamples) throws IOException {
        ensureOpen();
        if (maxSamples <= 0) {
            throw new IllegalArgumentException("maxSamples must be positive: " + maxSamples);
        }

        List<byte[]> payloads = new ArrayList<>();
        int corruptedFramesSkipped = 0;
        while (payloads.size() < maxSamples) {
            if (currentSegment == null && !openContainingSegment()) break;

            int needed = maxSamples - payloads.size();
            int beforeSize = payloads.size();
            long newOffset = currentSegment.scan(currentOffset, needed, payloads::add);
            int consumedThisIter = payloads.size() - beforeSize;
            currentOffset = newOffset;

            boolean reachedSegEnd = currentOffset >= currentSegment.endOffset();
            boolean boundedByQuota = consumedThisIter >= needed;

            if (reachedSegEnd) {
                long segStart = currentSegment.startOffset();
                currentSegment.close();
                currentSegment = null;
                // End of current segment. If no strictly-later segment
                // exists on disk, the reader is caught up to the writer
                // — break to avoid spinning on openContainingSegment
                // picking the same segment again.
                if (!hasLaterSegment(segStart)) break;
            } else if (boundedByQuota) {
                // Read exactly the requested number within this segment.
                // Leave currentSegment open for the next nextBatch call.
                break;
            } else {
                // Stopped mid-segment without hitting the quota — scan
                // encountered a torn frame. Two cases:
                //
                //   (a) Sealed segment (a strictly-later segment exists):
                //       the torn frame is permanent bit-rot. Skip past
                //       this segment with a WARN so later segments
                //       remain deliverable. Samples between the torn
                //       frame and segment end are lost — operator sees
                //       this via the WARN; a counter will surface it in
                //       §10's metrics layer.
                //
                //   (b) Active segment (no later segment yet): the torn
                //       frame may be a transient mid-append visibility
                //       effect. Don't skip; the writer will either
                //       complete the frame or the next recovery will
                //       truncate it.
                long segEnd = currentSegment.endOffset();
                long segStart = currentSegment.startOffset();
                if (hasLaterSegment(segStart)) {
                    LOG.warn("torn or corrupt frame in sealed WAL segment {} at offset {} — "
                            + "skipping to next segment; {} bytes of data in this segment may be lost",
                            currentSegment.segPath().getFileName(), currentOffset,
                            segEnd - currentOffset);
                    // We don't know the exact frame count lost (the
                    // segment is corrupt past this point), so the
                    // counter is bumped by 1 per skip — operators see
                    // "N corrupt-frame events," not "N samples lost."
                    // Pair this with the WARN log for the byte count.
                    corruptedFramesSkipped++;
                    currentOffset = segEnd;
                    currentSegment.close();
                    currentSegment = null;
                    continue;
                } else {
                    break;
                }
            }
        }

        return new ReadResult(payloads, currentOffset, corruptedFramesSkipped);
    }

    /** True if any segment on disk has startOffset > the given offset. */
    private boolean hasLaterSegment(long relativeToStart) throws IOException {
        for (long s : listSegmentStartOffsets()) {
            if (s > relativeToStart) return true;
        }
        return false;
    }

    /**
     * Advance the read offset forward (used when a batch was confirmed
     * shipped and the checkpoint moves). Typically equal to
     * {@link ReadResult#newOffset()} from the last {@link #nextBatch},
     * but can be any valid offset ≥ {@link #currentOffset()}.
     */
    public void advanceTo(long newOffset) throws IOException {
        ensureOpen();
        if (newOffset < currentOffset) {
            throw new IllegalArgumentException(
                "cannot advance backwards: currentOffset=" + currentOffset
                + ", requested=" + newOffset);
        }
        currentOffset = newOffset;
        if (currentSegment != null && currentOffset >= currentSegment.endOffset()) {
            currentSegment.close();
            currentSegment = null;
        }
    }

    /**
     * Open the segment whose offset range contains {@link #currentOffset}
     * and set {@link #currentSegment}. Iterative — on a failed open (file
     * vanished between list and open, or currentOffset is past the
     * chosen segment's end) advances to the next available segment
     * until success or no candidates remain.
     *
     * <p>Only mutates {@link #currentOffset} and {@link #currentSegment}
     * on success, or to advance past a drop-oldest eviction that left
     * the reader's previous offset inside a now-deleted segment (the
     * samples_dropped_wal_full counter was already ticked at eviction
     * time; the reader signals "lost data" via a WARN log here).
     */
    private boolean openContainingSegment() throws IOException {
        long attemptOffset = currentOffset;
        // Bounded iteration: at most one pass per segment on disk. A
        // misbehaving filesystem that keeps removing segments underneath
        // us is still bounded by the initial listing size.
        List<Long> starts = listSegmentStartOffsets();
        if (starts.isEmpty()) return false;

        int maxAttempts = starts.size() + 1;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            Long chosen = pickSegmentForOffset(starts, attemptOffset);
            if (chosen == null) {
                // currentOffset is before every surviving segment (oldest
                // segments were evicted under drop-oldest while this
                // reader was behind). Jump forward to the oldest segment
                // that still exists; data between currentOffset and that
                // start is gone — log it and bump forward.
                long oldestSurviving = starts.get(0);
                LOG.warn("reader offset {} is before the oldest surviving WAL segment "
                        + "(start {}) — {} bytes of data were evicted under the "
                        + "drop-oldest policy and are permanently lost",
                        attemptOffset, oldestSurviving, oldestSurviving - attemptOffset);
                attemptOffset = oldestSurviving;
                continue;
            }

            Path segPath = WalSegment.segPathFor(dir, chosen);
            WalSegment seg;
            try {
                seg = WalSegment.openForRead(segPath, chosen, maxPayload);
            } catch (NoSuchFileException vanished) {
                // Raced an eviction — rebuild the listing and try again.
                starts = listSegmentStartOffsets();
                if (starts.isEmpty()) return false;
                continue;
            }

            if (attemptOffset > seg.endOffset()) {
                // The chosen segment's offset range is entirely behind
                // us. Advance to the next segment (if any) and retry.
                seg.close();
                Long next = pickNextSegment(starts, chosen);
                if (next == null) return false; // no later segment
                attemptOffset = Math.max(attemptOffset, next);
                continue;
            }

            // Success — commit state.
            currentSegment = seg;
            currentOffset = attemptOffset;
            return true;
        }
        return false;
    }

    /** Largest start offset in {@code starts} that is {@code <= offset}, or null. */
    private static Long pickSegmentForOffset(List<Long> starts, long offset) {
        for (int i = starts.size() - 1; i >= 0; i--) {
            long s = starts.get(i);
            if (s <= offset) return s;
        }
        return null;
    }

    /** First start offset strictly greater than {@code chosen}, or null. */
    private static Long pickNextSegment(List<Long> starts, long chosen) {
        for (long s : starts) {
            if (s > chosen) return s;
        }
        return null;
    }

    private List<Long> listSegmentStartOffsets() throws IOException {
        List<Long> out = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + WalSegment.SEG_EXT)) {
            for (Path p : stream) {
                long s = WalSegment.parseStartOffset(p);
                if (s >= 0) out.add(s);
            }
        }
        Collections.sort(out);
        return out;
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        if (currentSegment != null) {
            currentSegment.close();
            currentSegment = null;
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("reader is closed");
    }

    /**
     * Result of a {@link #nextBatch(int)} call.
     *
     * @param payloads               frames decoded successfully
     * @param newOffset              global offset to checkpoint after a
     *                               successful ship of these payloads
     * @param corruptedFramesSkipped count of sealed segments the reader
     *                               skipped past during this batch due
     *                               to torn / bad-CRC frames mid-segment.
     *                               Caller increments
     *                               {@code wal_frames_dropped_corrupted_total}
     *                               by this amount.
     */
    public record ReadResult(List<byte[]> payloads, long newOffset, int corruptedFramesSkipped) {
        public boolean isEmpty() { return payloads.isEmpty(); }
        public int size() { return payloads.size(); }
    }
}
