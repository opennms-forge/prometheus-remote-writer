/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        while (payloads.size() < maxSamples) {
            if (currentSegment == null && !openContainingSegment()) break;

            int needed = maxSamples - payloads.size();
            int beforeSize = payloads.size();
            long newOffset = currentSegment.scan(currentOffset, needed, payloads::add);
            boolean readAny = payloads.size() > beforeSize;
            currentOffset = newOffset;

            if (currentOffset >= currentSegment.endOffset()) {
                long segStart = currentSegment.startOffset();
                currentSegment.close();
                currentSegment = null;
                // We've reached the end of the current segment. Check
                // whether a STRICTLY LATER segment exists; if not, the
                // reader is caught up to the writer and we break to
                // avoid spinning on openContainingSegment picking the
                // same segment again (which would re-open, scan 0
                // frames, close, repeat — the infinite-loop trap).
                if (!hasLaterSegment(segStart)) break;
            } else if (!readAny) {
                // Scan stopped mid-segment without reading anything —
                // either a torn frame or the writer hasn't committed new
                // bytes since our last scan. Don't spin.
                break;
            } else {
                // Bounded by maxSamples within the segment. We're still
                // mid-segment; leave currentSegment open for the next
                // nextBatch call.
                break;
            }
        }

        return new ReadResult(payloads, currentOffset);
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

    /** Open the segment whose offset range contains {@link #currentOffset}. */
    private boolean openContainingSegment() throws IOException {
        List<Long> starts = listSegmentStartOffsets();
        if (starts.isEmpty()) return false;

        Long chosen = null;
        for (int i = starts.size() - 1; i >= 0; i--) {
            long s = starts.get(i);
            if (s <= currentOffset) {
                chosen = s;
                break;
            }
        }
        if (chosen == null) {
            throw new IllegalStateException(
                "no segment contains offset " + currentOffset + " (earliest start: "
                + starts.get(0) + "). WAL state is inconsistent — checkpoint may "
                + "reference a segment that was already deleted.");
        }

        Path segPath = WalSegment.segPathFor(dir, chosen);
        WalSegment seg = WalSegment.openForRead(segPath, chosen, maxPayload);
        // Guard: if currentOffset lies past this segment's end (possible
        // if older segments were deleted after drop-oldest eviction),
        // close it and try the next one up.
        if (currentOffset > seg.endOffset()) {
            seg.close();
            // Find the next segment above chosen, if any.
            for (long s : starts) {
                if (s > chosen) {
                    currentOffset = Math.max(currentOffset, s);
                    return openContainingSegment(); // recurse with updated offset
                }
            }
            return false;
        }
        currentSegment = seg;
        return true;
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

    /** Result of a {@link #nextBatch(int)} call. */
    public record ReadResult(List<byte[]> payloads, long newOffset) {
        public boolean isEmpty() { return payloads.isEmpty(); }
        public int size() { return payloads.size(); }
    }
}
