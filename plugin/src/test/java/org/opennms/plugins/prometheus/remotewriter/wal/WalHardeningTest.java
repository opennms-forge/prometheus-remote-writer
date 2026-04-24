/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader.ReadResult;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy;

/**
 * Tests for the post-review hardening: eviction reader-floor, reader
 * torn-sealed-segment skip, recursion-free openContainingSegment.
 */
class WalHardeningTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    // --- H2: DROP_OLDEST respects reader floor ------------------------------

    @Test
    void drop_oldest_refuses_to_evict_segments_past_the_reader_floor(@TempDir Path dir)
            throws IOException {
        // Tight cap so eviction fires quickly.
        long segmentSize = 200;
        long maxSize = 300;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, maxSize,
                OverflowPolicy.DROP_OLDEST, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            // Fill segment 0 and rotate. After 2 appends: seg 0 sealed
            // (216 bytes), seg 1 active (0 bytes). currentTotal=216.
            for (int i = 0; i < 2; i++) w.append(payload);

            // Set the reader floor at offset 108 (reader has consumed
            // only the first frame of seg 0). Seg 0's endOffset is 216
            // > 108 — eviction would cross the floor. The third append
            // triggers overflow (324 > 300) → evictor refuses → throws.
            w.setReaderOffsetFloor(108);
            assertThatThrownBy(() -> w.append(payload))
                    .isInstanceOf(WalFullException.class)
                    .hasMessageContaining("no evictable segments");
        }
    }

    @Test
    void drop_oldest_evicts_once_reader_floor_advances(@TempDir Path dir)
            throws IOException {
        long segmentSize = 200;
        long maxSize = 300;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, maxSize,
                OverflowPolicy.DROP_OLDEST, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            for (int i = 0; i < 3; i++) w.append(payload);

            // Reader catches up past the oldest segment — now eviction
            // is safe. Floor past segment 0's end (any large value).
            w.setReaderOffsetFloor(Long.MAX_VALUE / 2);
            w.append(payload); // must succeed (eviction fires, append lands)
            assertThat(w.currentTotalBytes()).isLessThanOrEqualTo(maxSize);
        }
    }

    @Test
    void floor_of_zero_is_the_default_library_behavior(@TempDir Path dir) throws IOException {
        // Explicitly verify: without setReaderOffsetFloor, eviction
        // proceeds as before (matches the standalone library use case).
        long segmentSize = 200;
        long maxSize = 300;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, maxSize,
                OverflowPolicy.DROP_OLDEST, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            for (int i = 0; i < 10; i++) w.append(payload);
            assertThat(w.currentTotalBytes()).isLessThanOrEqualTo(maxSize);
        }
    }

    // --- H3: reader tolerates segment vanishing between list and open -------

    @Test
    void reader_advances_past_deleted_segment_without_crashing(@TempDir Path dir)
            throws IOException {
        // Populate with enough frames to span multiple segments.
        try (WalWriter w = WalWriter.createNew(dir, 200, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 6; i++) w.append(new byte[100]);
        }

        // Open a reader, consume 2 frames (we're now inside segment 0
        // or so — offset ≈ 216).
        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            ReadResult first = r.nextBatch(2);
            assertThat(first.size()).isEqualTo(2);

            // Out-of-band: delete the oldest segment while the reader is
            // mid-drain. (Simulates drop-oldest eviction ignoring the
            // floor — or an operator hand-deleting a segment. The reader
            // must survive gracefully: log a WARN, advance past the
            // deleted range to the next available segment, continue.)
            long oldest = firstSegmentStart(dir);
            Files.deleteIfExists(WalSegment.segPathFor(dir, oldest));
            Files.deleteIfExists(WalSegment.idxPathFor(dir, oldest));

            // Next batch: reader advances past the (now-missing) segment
            // and continues from the next one. Exact count depends on
            // how much was in segment 0 vs the successors; key
            // invariant: no exception.
            ReadResult next = r.nextBatch(100);
            assertThat(next).isNotNull();
            // Reader keeps functioning — can ask again for more.
            ReadResult tail = r.nextBatch(100);
            assertThat(tail).isNotNull();
        }
    }

    // --- H5: torn sealed segment is skipped with WARN, not a stall ----------

    @Test
    void reader_skips_torn_frame_in_sealed_segment(@TempDir Path dir) throws IOException {
        // Build a WAL with segment 0 (sealed, 2 frames × 108 bytes)
        // plus segment 1 (active, 1 frame). Payloads are 100 bytes each
        // so frames are 108 bytes and rotation fires after the 2nd.
        long segmentSize = 200;
        byte[] p1 = padName("seg0-frame1", 100);
        byte[] p2 = padName("seg0-frame2", 100);
        byte[] p3 = padName("seg1-frame1", 100);
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append(p1);
            w.append(p2); // triggers rotation post-append
            w.append(p3);
        }

        // Flip the very last byte of segment 0 (inside the trailing CRC
        // of the second frame) so scan hits a torn frame AFTER reading
        // the first frame.
        Path seg0 = WalSegment.segPathFor(dir, 0);
        long size0 = Files.size(seg0);
        try (FileChannel ch = FileChannel.open(seg0, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ch.position(size0 - 1);
            ByteBuffer b = ByteBuffer.allocate(1);
            ch.read(b);
            byte flipped = (byte) (b.get(0) ^ (byte) 0xff);
            ch.position(size0 - 1);
            ch.write(ByteBuffer.wrap(new byte[]{flipped}));
        }

        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            List<byte[]> collected = new ArrayList<>();
            for (int i = 0; i < 10 && collected.size() < 3; i++) {
                ReadResult batch = r.nextBatch(10);
                if (batch.isEmpty()) break;
                collected.addAll(batch.payloads());
            }
            // Segment 0's first frame is readable; the second frame's
            // CRC is broken; reader skips past the sealed segment with a
            // WARN and delivers the frame from segment 1.
            List<String> prefixes = collected.stream()
                    .map(bs -> new String(bs, 0, "seg0-frame0".length()))
                    .toList();
            assertThat(prefixes).contains("seg0-frame1", "seg1-frame1");
        }
    }

    /** Pad a short name to {@code totalLen} bytes so the payload triggers rotation. */
    private static byte[] padName(String name, int totalLen) {
        byte[] out = new byte[totalLen];
        byte[] n = name.getBytes();
        System.arraycopy(n, 0, out, 0, Math.min(n.length, totalLen));
        return out;
    }

    @Test
    void reader_does_not_skip_torn_frame_in_active_segment(@TempDir Path dir)
            throws IOException {
        // Reverse scenario: if the torn frame is in the ACTIVE segment
        // (no later segment exists), the reader must NOT skip — it may
        // be that the writer is just mid-append and the torn frame is
        // transient. Reader should break out and return what it has.
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("good-1".getBytes());
            w.append("good-2".getBytes());
        }
        // Append a torn frame to segment 0 (which remains the active /
        // only segment in this test).
        Path seg0 = WalSegment.segPathFor(dir, 0);
        try (FileChannel ch = FileChannel.open(seg0, StandardOpenOption.WRITE)) {
            ch.position(ch.size());
            ByteBuffer hdr = ByteBuffer.allocate(4);
            hdr.putInt(200).flip();
            ch.write(hdr);
            ch.write(ByteBuffer.wrap(new byte[]{1, 2, 3}));
        }

        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            ReadResult batch = r.nextBatch(100);
            // Two good frames readable; torn frame at the end of the
            // active segment is left for recovery to handle (and does
            // not trigger a skip-and-WARN).
            List<String> strings = batch.payloads().stream().map(String::new).toList();
            assertThat(strings).containsExactly("good-1", "good-2");
        }
    }

    // --- H4: rotate() failure leaves writer recoverable ---------------------

    @Test
    void writer_survives_rotation_failure_with_stale_file(@TempDir Path dir) throws IOException {
        // Pre-plant a stale file at the expected next-segment path so
        // WalSegment.create's CREATE_NEW fails during rotation. This
        // simulates a crashed prior rotation that left an orphan file.
        long segmentSize = 200;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            w.append(payload);               // seg 0: 108 bytes, no rotation
            // Pre-plant the file that rotation will try to create next.
            // The expected next startOffset will be active.endOffset()
            // after the NEXT append = 216. Plant "00...000216.seg".
            Files.createFile(WalSegment.segPathFor(dir, 216));

            // This append pushes seg 0 past the threshold — triggers
            // rotate() which tries to create segment starting at 216,
            // fails. Writer must NOT be permanently wedged.
            assertThatThrownBy(() -> w.append(payload))
                    .isInstanceOf(IOException.class);

            // Remove the obstacle and retry — writer should now rotate
            // successfully (the previous append to seg 0 actually
            // landed; only the subsequent rotate failed).
            Files.deleteIfExists(WalSegment.segPathFor(dir, 216));
            w.append(payload); // success now
        }
    }

    // --- Helpers ------------------------------------------------------------

    private static long firstSegmentStart(Path dir) throws IOException {
        return Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(WalSegment.SEG_EXT))
                .mapToLong(WalSegment::parseStartOffset)
                .filter(s -> s >= 0)
                .min()
                .orElseThrow();
    }
}
