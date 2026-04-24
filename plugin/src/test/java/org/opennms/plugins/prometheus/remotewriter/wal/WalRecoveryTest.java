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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalRecovery.RecoveredWal;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy;

class WalRecoveryTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    @Test
    void recover_from_empty_directory_creates_segment_zero(@TempDir Path dir) throws IOException {
        RecoveredWal r = WalRecovery.recover(dir, FsyncPolicy.BATCH, MAX_PAYLOAD);
        try (WalSegment seg = r.activeSegment()) {
            assertThat(seg.startOffset()).isZero();
            assertThat(seg.endOffset()).isZero();
            assertThat(seg.sampleCount()).isZero();
        }
        assertThat(r.checkpoint().lastSentOffset()).isZero();
        assertThat(r.pendingSampleCount()).isZero();
        assertThat(r.totalBytesOnDisk()).isZero();
    }

    @Test
    void recover_creates_missing_directory(@TempDir Path parent) throws IOException {
        Path walDir = parent.resolve("wal-subdir");
        assertThat(Files.exists(walDir)).isFalse();
        RecoveredWal r = WalRecovery.recover(walDir, FsyncPolicy.BATCH, MAX_PAYLOAD);
        r.activeSegment().close();
        assertThat(Files.exists(walDir)).isTrue();
        assertThat(Files.isDirectory(walDir)).isTrue();
    }

    @Test
    void recover_after_clean_shutdown_replays_pending(@TempDir Path dir) throws IOException {
        // Populate WAL.
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 5; i++) w.append(("sample-" + i).getBytes());
        }
        // Recover — no checkpoint was written, so everything is pending.
        RecoveredWal r = WalRecovery.recover(dir, FsyncPolicy.BATCH, MAX_PAYLOAD);
        try (WalSegment seg = r.activeSegment()) {
            assertThat(seg.sampleCount()).isEqualTo(5);
        }
        assertThat(r.pendingSampleCount()).isEqualTo(5);
        assertThat(r.checkpoint().lastSentOffset()).isZero();
        assertThat(r.totalBytesOnDisk()).isPositive();
    }

    @Test
    void recover_preserves_existing_checkpoint(@TempDir Path dir) throws IOException {
        // Set up a WAL with 3 samples shipped (checkpoint advanced past
        // them) and 2 still pending.
        long checkpointOffset;
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 3; i++) w.append(("shipped-" + i).getBytes());
            checkpointOffset = w.currentOffset();
            for (int i = 0; i < 2; i++) w.append(("pending-" + i).getBytes());
        }
        Checkpoint cp = Checkpoint.loadOrCreate(dir);
        cp.advance(checkpointOffset);

        RecoveredWal r = WalRecovery.recover(dir, FsyncPolicy.BATCH, MAX_PAYLOAD);
        r.activeSegment().close();
        assertThat(r.checkpoint().lastSentOffset()).isEqualTo(checkpointOffset);
        // pendingSampleCount is approximate — when the checkpoint lies
        // inside a segment, the whole segment is counted. For this test
        // we have ONE segment containing 5 samples; checkpoint is
        // mid-segment. So pending is reported as 5 even though only 2
        // are truly pending. This is the documented approximation.
        assertThat(r.pendingSampleCount()).isEqualTo(5);
    }

    @Test
    void recover_truncates_torn_tail(@TempDir Path dir) throws IOException {
        // Build a valid WAL with 3 frames.
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 3; i++) w.append(("good-" + i).getBytes());
        }
        // Append a torn frame to segment 0 (simulates crash mid-append).
        Path segFile = WalSegment.segPathFor(dir, 0);
        long preTornSize = Files.size(segFile);
        try (FileChannel ch = FileChannel.open(segFile, StandardOpenOption.WRITE)) {
            ch.position(ch.size());
            ByteBuffer hdr = ByteBuffer.allocate(4);
            hdr.putInt(200).flip();
            ch.write(hdr);
            ch.write(ByteBuffer.wrap(new byte[]{1, 2, 3}));
        }
        assertThat(Files.size(segFile)).isGreaterThan(preTornSize);

        RecoveredWal r = WalRecovery.recover(dir, FsyncPolicy.BATCH, MAX_PAYLOAD);
        try (WalSegment seg = r.activeSegment()) {
            // Segment has been truncated back to the last good frame.
            assertThat(seg.endOffset()).isEqualTo(preTornSize);
            assertThat(seg.sampleCount()).isEqualTo(3);
            assertThat(seg.status()).isEqualTo(WalSegment.Status.TORN);
        }
        // .idx reflects the torn status after recovery rewrites it.
        String idx = Files.readString(WalSegment.idxPathFor(dir, 0));
        assertThat(idx).contains("\"status\":\"torn\"");
    }

    @Test
    void recover_then_writer_resume_continues_cleanly(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("before".getBytes());
        }
        RecoveredWal r = WalRecovery.recover(dir, FsyncPolicy.BATCH, MAX_PAYLOAD);
        try (WalWriter w = WalWriter.resume(dir, r.activeSegment(), 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("after".getBytes());
        }
        // Read back: both samples present in order.
        try (WalReader rd = new WalReader(dir, 0, MAX_PAYLOAD)) {
            var batch = rd.nextBatch(10);
            assertThat(batch.size()).isEqualTo(2);
            assertThat(new String(batch.payloads().get(0))).isEqualTo("before");
            assertThat(new String(batch.payloads().get(1))).isEqualTo("after");
        }
    }

    @Test
    void recover_fails_when_checkpoint_is_corrupt(@TempDir Path dir) throws IOException {
        // Make a WAL then corrupt checkpoint.json.
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("x".getBytes());
        }
        Checkpoint.loadOrCreate(dir).advance(10);
        Files.writeString(dir.resolve(Checkpoint.FILE_NAME),
                "{\"last_sent_offset\":\"this-is-wrong\"}\n");

        assertThatThrownBy(() -> WalRecovery.recover(dir, FsyncPolicy.BATCH, MAX_PAYLOAD))
                .isInstanceOf(IOException.class);
    }

    @Test
    void recover_fails_when_checkpoint_is_past_newest_segment(@TempDir Path dir)
            throws IOException {
        // Create a segment with 1 frame, then write a checkpoint claiming
        // offset way past its end. WAL is inconsistent.
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("x".getBytes());
        }
        Checkpoint cp = Checkpoint.loadOrCreate(dir);
        cp.advance(10_000_000L); // way past anything real

        assertThatThrownBy(() -> WalRecovery.recover(dir, FsyncPolicy.BATCH, MAX_PAYLOAD))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("past");
    }

    @Test
    void recover_accepts_multiple_segments_and_counts_pending(@TempDir Path dir)
            throws IOException {
        // Force rotation by using a small segmentSize. 3 segments with
        // 2 frames each = 6 total samples.
        try (WalWriter w = WalWriter.createNew(dir, 250, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 6; i++) w.append(new byte[100]);
        }
        RecoveredWal r = WalRecovery.recover(dir, FsyncPolicy.BATCH, MAX_PAYLOAD);
        r.activeSegment().close();
        // All 6 samples pending (no checkpoint advance).
        assertThat(r.pendingSampleCount()).isEqualTo(6);
        assertThat(r.totalBytesOnDisk()).isGreaterThan(0);
    }

    @Test
    void recover_on_existing_file_that_is_not_a_directory_fails(@TempDir Path parent)
            throws IOException {
        Path file = parent.resolve("wal");
        Files.createFile(file); // a regular file, not a directory
        assertThatThrownBy(() -> WalRecovery.recover(file, FsyncPolicy.BATCH, MAX_PAYLOAD))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not a directory");
    }
}
