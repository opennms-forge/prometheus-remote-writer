/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;

/**
 * Single-writer into the WAL directory. Owns the active
 * {@link WalSegment}, appends incoming payloads, and rotates to a new
 * segment once the active one exceeds the configured size threshold.
 *
 * <p>Thread-safety: {@code append} and {@code flush} are synchronized;
 * callers must still coordinate externally for any invariant that spans
 * more than one append (there is none today). Concurrent reads via
 * {@link WalReader} against the same directory are safe — they use
 * independent {@link java.nio.channels.FileChannel}s per segment.
 *
 * <p>Does not manage overflow (that's handled by the caller — {@code
 * PrometheusRemoteWriterStorage} intercepts on the {@code store()} path);
 * does not manage recovery (use {@link WalRecovery} before constructing).
 */
public final class WalWriter implements Closeable {

    private final Path dir;
    private final long segmentSizeBytes;
    private final FsyncPolicy fsync;
    private final int maxPayload;

    private WalSegment active;
    private boolean closed;

    /**
     * Normally constructed through one of the factory methods; tests
     * occasionally use the direct ctor.
     */
    public WalWriter(Path dir, WalSegment initialActiveSegment, long segmentSizeBytes,
                     FsyncPolicy fsync, int maxPayload) {
        if (initialActiveSegment == null) {
            throw new IllegalArgumentException("initialActiveSegment must not be null — "
                + "use createNew() or resume() to obtain one");
        }
        this.dir = dir;
        this.active = initialActiveSegment;
        this.segmentSizeBytes = segmentSizeBytes;
        this.fsync = fsync;
        this.maxPayload = maxPayload;
    }

    /**
     * Bootstrap a brand-new WAL in an empty directory: opens segment 0
     * and returns a writer positioned at global offset 0.
     */
    public static WalWriter createNew(Path dir, long segmentSizeBytes, FsyncPolicy fsync,
                                      int maxPayload) throws IOException {
        WalSegment seg = WalSegment.create(dir, 0, fsync, maxPayload);
        return new WalWriter(dir, seg, segmentSizeBytes, fsync, maxPayload);
    }

    /**
     * Resume into an existing, already-recovered segment. The segment is
     * expected to be open for append (see
     * {@link WalSegment#openForAppend}); rotation happens here when its
     * size exceeds the threshold.
     */
    public static WalWriter resume(Path dir, WalSegment activeSegment, long segmentSizeBytes,
                                   FsyncPolicy fsync, int maxPayload) {
        return new WalWriter(dir, activeSegment, segmentSizeBytes, fsync, maxPayload);
    }

    /**
     * Append one encoded payload. Returns the global offset immediately
     * past the written frame (inclusive end; exclusive upper bound).
     * Rotates to a new segment if the active one exceeds
     * {@code segmentSizeBytes} after the append — rotation is
     * post-append, so a single very-large frame may leave the active
     * segment somewhat above the threshold before rotation fires.
     */
    public synchronized long append(byte[] payload) throws IOException {
        ensureOpen();
        long offsetAfter = active.append(payload);
        if (active.endOffset() - active.startOffset() >= segmentSizeBytes) {
            rotate();
        }
        return offsetAfter;
    }

    /**
     * Force the active segment's fsync policy to act now, regardless of
     * which policy is configured. No-op under
     * {@link FsyncPolicy#NEVER} (by policy). Called at flush-interval
     * boundaries from the flusher thread.
     */
    public synchronized void flush() throws IOException {
        ensureOpen();
        active.flush();
    }

    /**
     * The global offset where the next append will land. Equal to the
     * active segment's {@link WalSegment#endOffset()}.
     */
    public synchronized long currentOffset() throws IOException {
        ensureOpen();
        return active.endOffset();
    }

    /** The active segment's directory — exposed for reader construction. */
    public Path dir() { return dir; }

    /**
     * Rotate: seal the current segment, create a new one starting where
     * the old one ended.
     */
    private void rotate() throws IOException {
        long nextStart = active.endOffset();
        active.close(); // seals and rewrites .idx with final state
        active = WalSegment.create(dir, nextStart, fsync, maxPayload);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        active.close();
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("writer is closed");
    }
}
