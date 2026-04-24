/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.function.Consumer;

/**
 * One WAL segment file on disk. A segment is a flat sequence of frames
 * (see {@link Frame}) covering a contiguous range of the global WAL
 * offset space. The {@link WalWriter} owns the active segment for
 * append, rotates to a new segment when this one crosses the configured
 * size threshold, and opens older segments read-only through the
 * {@link WalReader}.
 *
 * <p>Global offsets: the segment's {@link #startOffset} is the byte
 * position where its first frame sits in the WAL's global address space.
 * An offset N returned from {@link #append(byte[])} is the global offset
 * where the NEXT frame would land — i.e., an exclusive upper bound that
 * the checkpoint can safely record as "bytes 0..N-1 have been sent."
 *
 * <p>Fsync policy controls when {@link FileChannel#force(boolean)} fires
 * within {@link #append}:
 * <ul>
 *   <li>{@link FsyncPolicy#ALWAYS} — every append fsyncs. Highest
 *       durability; throughput floor on SSD ~10-30k appends/sec.</li>
 *   <li>{@link FsyncPolicy#BATCH} — {@link #append} does NOT fsync; caller
 *       must invoke {@link #flush()} explicitly, typically at the
 *       flush-interval boundary.</li>
 *   <li>{@link FsyncPolicy#NEVER} — neither {@link #append} nor
 *       {@link #flush()} forces to disk. Relies on OS page cache; loses
 *       pending writes on kernel panic or power loss.</li>
 * </ul>
 * {@link #close()} always fsyncs regardless of policy (best-effort final
 * durability on clean shutdown).
 *
 * <p>Not thread-safe — the writer enforces single-writer discipline
 * externally.
 */
public final class WalSegment implements Closeable {

    public enum FsyncPolicy { ALWAYS, BATCH, NEVER }

    /** File extensions — kept as constants so tests and recovery agree. */
    public static final String SEG_EXT = ".seg";
    public static final String IDX_EXT = ".idx";

    /** Segment filename grammar: 20-digit zero-padded start offset. */
    private static final String FILE_NAME_FORMAT = "%020d";

    private final Path segPath;
    private final Path idxPath;
    private final long startOffset;
    private final FileChannel channel;
    private final FsyncPolicy fsync;
    private final int maxPayload;
    private final Instant createdAt;

    private int sampleCount;
    private Status status;
    private boolean closed;

    /** Status mirrors the .idx file field. */
    public enum Status { OPEN, SEALED, TORN }

    private WalSegment(Path segPath, Path idxPath, long startOffset, FileChannel channel,
                       FsyncPolicy fsync, int maxPayload, Instant createdAt, int sampleCount,
                       Status status) {
        this.segPath = segPath;
        this.idxPath = idxPath;
        this.startOffset = startOffset;
        this.channel = channel;
        this.fsync = fsync;
        this.maxPayload = maxPayload;
        this.createdAt = createdAt;
        this.sampleCount = sampleCount;
        this.status = status;
    }

    // --- Factories ---------------------------------------------------------

    /**
     * Create a brand-new segment file in the given directory. Fails if
     * the file already exists. The companion .idx is written with
     * {@link Status#OPEN}.
     */
    public static WalSegment create(Path dir, long startOffset, FsyncPolicy fsync,
                                    int maxPayload) throws IOException {
        Path segPath = segPathFor(dir, startOffset);
        Path idxPath = idxPathFor(dir, startOffset);
        FileChannel ch = FileChannel.open(segPath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
        WalSegment seg = new WalSegment(segPath, idxPath, startOffset, ch, fsync, maxPayload,
                Instant.now(), 0, Status.OPEN);
        seg.writeIndex();
        return seg;
    }

    /**
     * Re-open an existing segment for append. Used after recovery to
     * resume writing the latest segment. The channel is positioned at
     * end-of-file; {@code sampleCount} is seeded from the caller's
     * recovery scan (the segment does not re-scan on re-open).
     */
    public static WalSegment openForAppend(Path segPath, long startOffset, FsyncPolicy fsync,
                                           int maxPayload, int sampleCountFromRecovery)
            throws IOException {
        Path idxPath = idxPathFor(segPath.getParent(), startOffset);
        FileChannel ch = FileChannel.open(segPath,
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        ch.position(ch.size());
        return new WalSegment(segPath, idxPath, startOffset, ch, fsync, maxPayload,
                Instant.now(), sampleCountFromRecovery, Status.OPEN);
    }

    /**
     * Open an existing segment read-only. Used by the reader for
     * already-sealed segments.
     */
    public static WalSegment openForRead(Path segPath, long startOffset, int maxPayload)
            throws IOException {
        Path idxPath = idxPathFor(segPath.getParent(), startOffset);
        FileChannel ch = FileChannel.open(segPath, StandardOpenOption.READ);
        return new WalSegment(segPath, idxPath, startOffset, ch, FsyncPolicy.NEVER, maxPayload,
                Instant.now(), 0, Status.SEALED);
    }

    // --- Ops ---------------------------------------------------------------

    /**
     * Append a framed payload. Returns the global offset immediately
     * past the written frame (exclusive upper bound) — this is the value
     * a checkpoint records as "sent through here."
     *
     * @throws IOException if the underlying write fails (including disk
     *                     full)
     */
    public long append(byte[] payload) throws IOException {
        ensureOpen();
        ByteBuffer frame = Frame.encode(payload);
        while (frame.hasRemaining()) channel.write(frame);
        if (fsync == FsyncPolicy.ALWAYS) channel.force(false);
        sampleCount++;
        return startOffset + channel.size();
    }

    /**
     * Force fsync regardless of fsync policy. Called by the writer on
     * flush-interval boundary (under BATCH policy) and at segment
     * rotation. No-op under {@link FsyncPolicy#NEVER}.
     */
    public void flush() throws IOException {
        ensureOpen();
        if (fsync != FsyncPolicy.NEVER) channel.force(false);
    }

    /**
     * Scan frames forward from {@code fromGlobalOffset}, invoking the
     * consumer on each payload. Stops at end-of-file OR at a torn frame.
     * Returns the global offset one past the last successfully-decoded
     * frame — the caller's new read position.
     *
     * <p>The segment channel is repositioned by this method. Do not
     * interleave {@link #append} and {@link #scan} calls — the writer
     * should not be concurrently reading its active segment.
     */
    public long scan(long fromGlobalOffset, Consumer<byte[]> consumer) throws IOException {
        ensureOpen();
        if (fromGlobalOffset < startOffset) {
            throw new IllegalArgumentException(
                "offset " + fromGlobalOffset + " is before segment start " + startOffset);
        }
        long fileOffset = fromGlobalOffset - startOffset;
        long fileSize = channel.size();
        if (fileOffset > fileSize) {
            throw new IllegalArgumentException(
                "offset " + fromGlobalOffset + " is past segment end " + (startOffset + fileSize));
        }
        channel.position(fileOffset);
        while (channel.position() < fileSize) {
            long beforeFrame = channel.position();
            byte[] payload = Frame.decode(channel, maxPayload);
            if (payload == null) {
                // Torn frame — stop. Channel is rewound to frame start by
                // Frame.decode.
                return startOffset + beforeFrame;
            }
            consumer.accept(payload);
        }
        return startOffset + channel.position();
    }

    /**
     * Recover the segment: scan from the beginning, count frames, detect
     * torn tail. On torn tail, truncate the file at the last good frame
     * and mark status as {@link Status#TORN}. Returns the recovered
     * {@link #endOffset()}.
     *
     * <p>Called during startup recovery; safe to call on a fresh or
     * already-sealed segment (no-op if no damage is found).
     */
    public long recover() throws IOException {
        ensureOpen();
        int framesSeen = 0;
        long lastGood = startOffset;
        long fileSize = channel.size();
        channel.position(0);
        while (channel.position() < fileSize) {
            byte[] payload = Frame.decode(channel, maxPayload);
            if (payload == null) {
                // Torn frame; truncate here.
                long truncateAt = channel.position();
                if (truncateAt < fileSize) {
                    channel.truncate(truncateAt);
                    channel.force(true);
                    status = Status.TORN;
                }
                lastGood = startOffset + truncateAt;
                break;
            }
            framesSeen++;
            lastGood = startOffset + channel.position();
        }
        if (status == Status.OPEN && channel.position() >= fileSize) {
            // Full scan completed cleanly — no torn tail. Leave status as OPEN
            // if still active; the writer (not recover) is responsible for
            // sealing.
            lastGood = startOffset + fileSize;
        }
        sampleCount = framesSeen;
        channel.position(channel.size());
        return lastGood;
    }

    /**
     * Write / refresh the companion .idx file with current segment state.
     * Tiny footprint; called at create, at close, and explicitly after
     * recovery. The .idx is hand-rolled JSON rather than a library call
     * to avoid pulling another dependency into the plugin for a
     * five-field record.
     */
    public void writeIndex() throws IOException {
        long endOffset = startOffset + (channel.isOpen() ? channel.size() : Files.size(segPath));
        String line = String.format(
                "{\"start_offset\":%d,\"end_offset\":%d,\"sample_count\":%d,"
              + "\"created_at\":\"%s\",\"status\":\"%s\"}%n",
                startOffset, endOffset, sampleCount, createdAt, status.name().toLowerCase());
        Files.writeString(idxPath, line);
    }

    // --- Accessors ---------------------------------------------------------

    public long startOffset() { return startOffset; }

    public long endOffset() throws IOException {
        ensureOpen();
        return startOffset + channel.size();
    }

    public int sampleCount() { return sampleCount; }

    public Path segPath() { return segPath; }

    public Path idxPath() { return idxPath; }

    public Status status() { return status; }

    // --- Lifecycle ---------------------------------------------------------

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            if (channel.isOpen()) channel.force(false);
            if (status == Status.OPEN) status = Status.SEALED;
            writeIndex();
        } finally {
            channel.close();
        }
    }

    // --- Helpers -----------------------------------------------------------

    private void ensureOpen() {
        if (closed) throw new IllegalStateException(
                "segment " + segPath.getFileName() + " is closed");
    }

    /** Resolve the .seg path for a given start offset in the directory. */
    public static Path segPathFor(Path dir, long startOffset) {
        return dir.resolve(String.format(FILE_NAME_FORMAT, startOffset) + SEG_EXT);
    }

    /** Resolve the .idx path for a given start offset in the directory. */
    public static Path idxPathFor(Path dir, long startOffset) {
        return dir.resolve(String.format(FILE_NAME_FORMAT, startOffset) + IDX_EXT);
    }

    /**
     * Parse the start offset out of a segment file name.
     * {@code 00000000000000000000.seg} → 0. Returns -1 if the name is not
     * a segment file.
     */
    public static long parseStartOffset(Path segPath) {
        String name = segPath.getFileName().toString();
        if (!name.endsWith(SEG_EXT)) return -1;
        String digits = name.substring(0, name.length() - SEG_EXT.length());
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
