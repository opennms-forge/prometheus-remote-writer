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
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.Status;

class WalSegmentTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    @Test
    void create_opens_an_empty_segment_and_writes_initial_idx(@TempDir Path dir) throws IOException {
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            assertThat(seg.startOffset()).isZero();
            assertThat(seg.endOffset()).isZero();
            assertThat(seg.sampleCount()).isZero();
            assertThat(seg.status()).isEqualTo(Status.OPEN);
            assertThat(Files.exists(seg.segPath())).isTrue();
            assertThat(Files.exists(seg.idxPath())).isTrue();
            String idx = Files.readString(seg.idxPath());
            assertThat(idx).contains("\"start_offset\":0")
                           .contains("\"sample_count\":0")
                           .contains("\"status\":\"open\"");
        }
    }

    @Test
    void append_writes_frames_advances_end_offset(@TempDir Path dir) throws IOException {
        try (WalSegment seg = WalSegment.create(dir, 1000, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            long endAfterFirst = seg.append("one".getBytes());
            long endAfterSecond = seg.append("two".getBytes());
            // Each frame: 4-byte length + 3-byte payload + 4-byte CRC = 11 bytes.
            assertThat(endAfterFirst).isEqualTo(1000 + 11);
            assertThat(endAfterSecond).isEqualTo(1000 + 22);
            assertThat(seg.endOffset()).isEqualTo(1000 + 22);
            assertThat(seg.sampleCount()).isEqualTo(2);
        }
    }

    @Test
    void scan_yields_appended_frames_in_order(@TempDir Path dir) throws IOException {
        List<byte[]> payloads = List.of("alpha".getBytes(), "beta".getBytes(), "gamma".getBytes());
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (byte[] p : payloads) seg.append(p);
            List<byte[]> collected = new ArrayList<>();
            long newOffset = seg.scan(0, collected::add);
            assertThat(collected).containsExactlyElementsOf(payloads);
            assertThat(newOffset).isEqualTo(seg.endOffset());
        }
    }

    @Test
    void scan_from_mid_segment_skips_earlier_frames(@TempDir Path dir) throws IOException {
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            long after1 = seg.append("one".getBytes());
            long after2 = seg.append("two".getBytes());
            seg.append("three".getBytes());

            List<byte[]> collected = new ArrayList<>();
            long newOffset = seg.scan(after1, collected::add);
            assertThat(collected).hasSize(2);
            assertThat(new String(collected.get(0))).isEqualTo("two");
            assertThat(new String(collected.get(1))).isEqualTo("three");
            assertThat(newOffset).isEqualTo(seg.endOffset());

            // A scan from after the LAST frame consumes nothing and
            // returns the same position.
            collected.clear();
            long terminalOffset = seg.scan(seg.endOffset(), collected::add);
            assertThat(collected).isEmpty();
            assertThat(terminalOffset).isEqualTo(seg.endOffset());
        }
    }

    @Test
    void scan_rejects_offset_before_segment_start(@TempDir Path dir) throws IOException {
        try (WalSegment seg = WalSegment.create(dir, 1000, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            seg.append("one".getBytes());
            assertThatThrownBy(() -> seg.scan(500, p -> {}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("before segment start");
        }
    }

    @Test
    void scan_rejects_offset_past_segment_end(@TempDir Path dir) throws IOException {
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            seg.append("one".getBytes());
            assertThatThrownBy(() -> seg.scan(10_000, p -> {}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("past segment end");
        }
    }

    @Test
    void close_is_idempotent_and_seals_the_status(@TempDir Path dir) throws IOException {
        WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD);
        seg.append("x".getBytes());
        seg.close();
        seg.close(); // second close is a no-op; does not throw
        String idx = Files.readString(seg.idxPath());
        assertThat(idx).contains("\"status\":\"sealed\"")
                       .contains("\"sample_count\":1");
    }

    @Test
    void append_after_close_throws(@TempDir Path dir) throws IOException {
        WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD);
        seg.close();
        assertThatThrownBy(() -> seg.append("x".getBytes()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void recover_on_clean_segment_counts_frames_and_keeps_status(@TempDir Path dir)
            throws IOException {
        Path segFile;
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            seg.append("one".getBytes());
            seg.append("two".getBytes());
            seg.append("three".getBytes());
            segFile = seg.segPath();
        }
        // Reopen for append (recovery scenario) and recover.
        try (WalSegment seg = WalSegment.openForAppend(segFile, 0, FsyncPolicy.BATCH, MAX_PAYLOAD, 0)) {
            long endOffset = seg.recover();
            assertThat(endOffset).isEqualTo(Files.size(segFile));
            assertThat(seg.sampleCount()).isEqualTo(3);
            assertThat(seg.status()).isEqualTo(Status.OPEN); // clean scan — still active
        }
    }

    @Test
    void recover_on_torn_tail_truncates_and_marks_torn(@TempDir Path dir) throws IOException {
        Path segFile;
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            seg.append("good-one".getBytes());
            seg.append("good-two".getBytes());
            segFile = seg.segPath();
        }
        // Append a torn frame: length header says 100, writes only 3 bytes.
        try (FileChannel ch = FileChannel.open(segFile, StandardOpenOption.WRITE)) {
            ch.position(ch.size());
            ByteBuffer hdr = ByteBuffer.allocate(4);
            hdr.putInt(100).flip();
            ch.write(hdr);
            ch.write(ByteBuffer.wrap(new byte[]{'a', 'b', 'c'}));
        }
        long preRecoverySize = Files.size(segFile);

        try (WalSegment seg = WalSegment.openForAppend(segFile, 0, FsyncPolicy.BATCH, MAX_PAYLOAD, 0)) {
            long endAfterRecover = seg.recover();
            long postRecoverySize = Files.size(segFile);
            assertThat(postRecoverySize).isLessThan(preRecoverySize);
            assertThat(endAfterRecover).isEqualTo(postRecoverySize);
            assertThat(seg.sampleCount()).isEqualTo(2);
            assertThat(seg.status()).isEqualTo(Status.TORN);
        }
    }

    @Test
    void append_after_recover_resumes_at_last_good_frame(@TempDir Path dir) throws IOException {
        Path segFile;
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            seg.append("kept".getBytes());
            segFile = seg.segPath();
        }
        // Induce a torn frame.
        try (FileChannel ch = FileChannel.open(segFile, StandardOpenOption.WRITE)) {
            ch.position(ch.size());
            ByteBuffer hdr = ByteBuffer.allocate(4);
            hdr.putInt(999).flip();
            ch.write(hdr);
        }

        try (WalSegment seg = WalSegment.openForAppend(segFile, 0, FsyncPolicy.BATCH, MAX_PAYLOAD, 0)) {
            seg.recover();
            seg.append("new-after-recovery".getBytes());
            // Scan everything; torn frame is gone, new frame is at the end.
            List<byte[]> collected = new ArrayList<>();
            seg.scan(0, collected::add);
            assertThat(collected).hasSize(2);
            assertThat(new String(collected.get(0))).isEqualTo("kept");
            assertThat(new String(collected.get(1))).isEqualTo("new-after-recovery");
        }
    }

    @Test
    void fsync_always_forces_every_append(@TempDir Path dir) throws IOException {
        // No mock — just verify the always path doesn't throw and produces
        // a visible on-disk file after each append. Actual fsync-behavior
        // testing would require a mocked FileChannel, out of scope for
        // unit tests. Integration tests validate under kill-sim.
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.ALWAYS, MAX_PAYLOAD)) {
            seg.append("one".getBytes());
            assertThat(Files.size(seg.segPath())).isEqualTo(11);
        }
    }

    @Test
    void fsync_never_skips_force_even_on_flush(@TempDir Path dir) throws IOException {
        // flush() is a no-op under NEVER policy. This is behavioral —
        // visible only by confirming the call doesn't throw and segment
        // remains usable.
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.NEVER, MAX_PAYLOAD)) {
            seg.append("x".getBytes());
            seg.flush();
            seg.append("y".getBytes());
            assertThat(seg.sampleCount()).isEqualTo(2);
        }
    }

    @Test
    void file_name_encodes_start_offset_for_listing(@TempDir Path dir) throws IOException {
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            assertThat(seg.segPath().getFileName().toString())
                    .isEqualTo("00000000000000000000.seg");
        }
        try (WalSegment seg = WalSegment.create(dir, 1_073_741_824L, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            assertThat(seg.segPath().getFileName().toString())
                    .isEqualTo("00000000001073741824.seg");
        }
    }

    @Test
    void parseStartOffset_round_trips_the_file_name_grammar(@TempDir Path dir) throws IOException {
        Path a = WalSegment.segPathFor(dir, 0);
        Path b = WalSegment.segPathFor(dir, 65_536_000);
        assertThat(WalSegment.parseStartOffset(a)).isEqualTo(0);
        assertThat(WalSegment.parseStartOffset(b)).isEqualTo(65_536_000);
        assertThat(WalSegment.parseStartOffset(dir.resolve("readme.txt"))).isEqualTo(-1);
        assertThat(WalSegment.parseStartOffset(dir.resolve("00000.idx"))).isEqualTo(-1);
        assertThat(WalSegment.parseStartOffset(dir.resolve("garbage.seg"))).isEqualTo(-1);
    }

    @Test
    void create_fails_when_file_already_exists(@TempDir Path dir) throws IOException {
        Path segPath = WalSegment.segPathFor(dir, 0);
        Files.createFile(segPath);
        assertThatThrownBy(() -> WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD))
                .isInstanceOf(IOException.class);
    }

    @Test
    void open_for_read_does_not_allow_append(@TempDir Path dir) throws IOException {
        Path segFile;
        try (WalSegment writer = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            writer.append("x".getBytes());
            segFile = writer.segPath();
        }
        try (WalSegment reader = WalSegment.openForRead(segFile, 0, MAX_PAYLOAD)) {
            // append() on a read-only channel throws NonWritableChannelException
            // (RuntimeException) from FileChannel.write — the JDK's way of
            // saying "this channel was opened READ-only." The WalSegment
            // does not wrap this; the stance is that a read-only segment is
            // a read-only segment, period.
            assertThatThrownBy(() -> reader.append("y".getBytes()))
                    .isInstanceOf(java.nio.channels.NonWritableChannelException.class);
        }
    }

    @Test
    void idx_file_reflects_final_sample_count_after_close(@TempDir Path dir) throws IOException {
        WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD);
        seg.append("1".getBytes());
        seg.append("2".getBytes());
        seg.append("3".getBytes());
        // Before close: idx is stale (written at create with 0 samples).
        String preClose = Files.readString(seg.idxPath());
        assertThat(preClose).contains("\"sample_count\":0");
        seg.close();
        // After close: idx reflects the three appended frames.
        String postClose = Files.readString(seg.idxPath());
        assertThat(postClose).contains("\"sample_count\":3")
                             .contains("\"status\":\"sealed\"");
    }
}
