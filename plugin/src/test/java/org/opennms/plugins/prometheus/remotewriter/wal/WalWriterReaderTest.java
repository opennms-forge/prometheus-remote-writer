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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader.ReadResult;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy;

class WalWriterReaderTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    @Test
    void writer_appends_and_reader_drains_in_order(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("one".getBytes());
            w.append("two".getBytes());
            w.append("three".getBytes());
            w.flush();
        }
        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            ReadResult batch = r.nextBatch(10);
            assertThat(batch.size()).isEqualTo(3);
            assertThat(asStrings(batch.payloads())).containsExactly("one", "two", "three");
        }
    }

    @Test
    void reader_respects_max_samples_bound(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 10; i++) w.append(("sample-" + i).getBytes());
            w.flush();
        }
        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            ReadResult first = r.nextBatch(3);
            assertThat(first.size()).isEqualTo(3);
            assertThat(asStrings(first.payloads())).containsExactly("sample-0", "sample-1", "sample-2");

            ReadResult second = r.nextBatch(3);
            assertThat(second.size()).isEqualTo(3);
            assertThat(asStrings(second.payloads())).containsExactly("sample-3", "sample-4", "sample-5");

            // Remaining 4 in a single larger ask
            ReadResult rest = r.nextBatch(100);
            assertThat(rest.size()).isEqualTo(4);
            assertThat(asStrings(rest.payloads()))
                    .containsExactly("sample-6", "sample-7", "sample-8", "sample-9");

            // Caught up — next call returns nothing.
            ReadResult empty = r.nextBatch(10);
            assertThat(empty.isEmpty()).isTrue();
        }
    }

    @Test
    void writer_rotates_when_segment_exceeds_size_threshold(@TempDir Path dir) throws IOException {
        // Pick a threshold small enough that ~3 frames trigger a rotation.
        // One frame = 8 bytes overhead + 100 bytes payload = 108 bytes.
        // Threshold = 250 bytes → rotation after the 3rd append.
        long segmentSize = 250;
        byte[] payload = new byte[100];
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 10; i++) w.append(payload);
            w.flush();
        }
        // Expect multiple .seg files on disk.
        long segCount;
        try (Stream<Path> s = Files.list(dir)) {
            segCount = s.filter(p -> p.getFileName().toString().endsWith(WalSegment.SEG_EXT)).count();
        }
        assertThat(segCount).isGreaterThan(1);
    }

    @Test
    void reader_crosses_segment_boundaries_transparently(@TempDir Path dir) throws IOException {
        // Write enough payloads to produce several segments.
        long segmentSize = 250;
        byte[] p = new byte[100];
        List<String> labels = new ArrayList<>();
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 15; i++) {
                String marker = "m-" + i;
                labels.add(marker);
                byte[] payload = new byte[100];
                byte[] m = marker.getBytes();
                System.arraycopy(m, 0, payload, 0, m.length);
                w.append(payload);
            }
            w.flush();
        }
        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            List<byte[]> all = new ArrayList<>();
            while (all.size() < 15) {
                ReadResult batch = r.nextBatch(4);
                if (batch.isEmpty()) break;
                all.addAll(batch.payloads());
            }
            assertThat(all).hasSize(15);
            for (int i = 0; i < 15; i++) {
                String prefix = new String(all.get(i), 0, labels.get(i).length());
                assertThat(prefix).isEqualTo(labels.get(i));
            }
        }
    }

    @Test
    void reader_sees_appends_made_after_reader_creation(@TempDir Path dir) throws IOException {
        // Live writer/reader pairing — reader opens while writer is still
        // appending to segment 0. The reader's per-nextBatch re-query of
        // channel.size() lets it see new bytes.
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
             WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {

            w.append("early".getBytes());
            w.flush();

            ReadResult batch1 = r.nextBatch(10);
            assertThat(asStrings(batch1.payloads())).containsExactly("early");

            w.append("late".getBytes());
            w.flush();

            ReadResult batch2 = r.nextBatch(10);
            assertThat(asStrings(batch2.payloads())).containsExactly("late");
        }
    }

    @Test
    void reader_starting_mid_wal_skips_earlier_data(@TempDir Path dir) throws IOException {
        long offsetAfterSecond;
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("one".getBytes());
            offsetAfterSecond = w.append("two".getBytes());
            w.append("three".getBytes());
            w.flush();
        }
        try (WalReader r = new WalReader(dir, offsetAfterSecond, MAX_PAYLOAD)) {
            ReadResult batch = r.nextBatch(10);
            assertThat(asStrings(batch.payloads())).containsExactly("three");
        }
    }

    @Test
    void advanceTo_moves_reader_forward_without_a_read(@TempDir Path dir) throws IOException {
        long offsetAfterFirst;
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            offsetAfterFirst = w.append("skip-me".getBytes());
            w.append("keep-me".getBytes());
            w.flush();
        }
        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            r.advanceTo(offsetAfterFirst);
            ReadResult batch = r.nextBatch(10);
            assertThat(asStrings(batch.payloads())).containsExactly("keep-me");
        }
    }

    @Test
    void advanceTo_rejects_backward_movement(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("x".getBytes());
            w.flush();
        }
        try (WalReader r = new WalReader(dir, 10, MAX_PAYLOAD)) {
            assertThatThrownBy(() -> r.advanceTo(5))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("backwards");
        }
    }

    @Test
    void reader_on_empty_directory_returns_empty_batch(@TempDir Path dir) throws IOException {
        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            ReadResult batch = r.nextBatch(10);
            assertThat(batch.isEmpty()).isTrue();
            assertThat(r.currentOffset()).isZero();
        }
    }

    @Test
    void writer_resume_continues_at_recovered_offset(@TempDir Path dir) throws IOException {
        // Write some data, close cleanly, then resume the writer against
        // the recovered segment (simulates a plugin restart with
        // in-flight writes).
        long endAfterFirst;
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("before-stop".getBytes());
            endAfterFirst = w.currentOffset();
        }
        // Resume: reopen the existing segment for append, count its
        // frames via recover, then construct a writer.
        Path segPath = WalSegment.segPathFor(dir, 0);
        WalSegment resumed = WalSegment.openForAppend(segPath, 0, FsyncPolicy.BATCH, MAX_PAYLOAD, 0);
        resumed.recover(); // no torn tail expected
        try (WalWriter w = WalWriter.resume(dir, resumed, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            long offset = w.append("after-resume".getBytes());
            assertThat(offset).isGreaterThan(endAfterFirst);
            w.flush();
        }
        // Read the whole WAL — both frames should be present in order.
        try (WalReader r = new WalReader(dir, 0, MAX_PAYLOAD)) {
            ReadResult all = r.nextBatch(100);
            assertThat(asStrings(all.payloads()))
                    .containsExactly("before-stop", "after-resume");
        }
    }

    @Test
    void closing_writer_seals_active_segment(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append("x".getBytes());
        }
        String idx = Files.readString(WalSegment.idxPathFor(dir, 0));
        assertThat(idx).contains("\"status\":\"sealed\"");
    }

    @Test
    void append_after_close_throws(@TempDir Path dir) throws IOException {
        WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
        w.close();
        assertThatThrownBy(() -> w.append("x".getBytes()))
                .isInstanceOf(IllegalStateException.class);
    }

    private static List<String> asStrings(List<byte[]> payloads) {
        return payloads.stream().map(String::new).toList();
    }
}
