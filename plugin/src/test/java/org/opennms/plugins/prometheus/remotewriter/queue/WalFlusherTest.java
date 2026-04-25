/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteOutcome;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteResult;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wal.Checkpoint;
import org.opennms.plugins.prometheus.remotewriter.wal.WalEntryCodec;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader.ReadResult;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * Focused tests for the post-Round-3 WalFlusher logic — counters
 * deferred until checkpoint persistence, rewind path semantics,
 * pending-sample tracking, and corruption-counter pass-through.
 *
 * <p>Uses a real WAL on a temp dir and a Mockito-mocked HTTP client so
 * outcomes are deterministic. Mockito's inline mock-maker (enabled by
 * default in the project's surefire config) handles RemoteWriteHttpClient
 * being a final class.
 */
class WalFlusherTest {

    private static final int MAX_PAYLOAD = 64 * 1024;
    private static final int BATCH_SIZE = 10;
    private static final long FLUSH_INTERVAL_MS = 50;

    @Test
    void success_path_ticks_counters_only_after_advance(@TempDir Path dir) throws IOException {
        seedSamples(dir, 3);
        PluginMetrics metrics = new PluginMetrics();
        WalWriter writer = WalWriter.resume(dir, openActive(dir), 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
        Checkpoint checkpoint = Checkpoint.loadOrCreate(dir);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        when(http.write(any())).thenReturn(new WriteResult(WriteOutcome.SUCCESS, 200, 1, null));

        try (WalFlusher flusher = new WalFlusher(dir, writer, checkpoint, MAX_PAYLOAD,
                http, BATCH_SIZE, FLUSH_INTERVAL_MS, metrics)) {
            flusher.flushBatch(readOneBatch(dir, checkpoint));

            // Counters ticked AFTER successful advance.
            assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue())
                    .isEqualTo(3);
            assertThat(metrics.snapshot().get(PluginMetrics.WAL_BYTES_CHECKPOINTED).longValue())
                    .isPositive();
            // Pending counter cleared.
            assertThat(flusher.pendingSampleCount()).isZero();
            // Checkpoint advanced past everything.
            assertThat(checkpoint.lastSentOffset()).isEqualTo(writer.currentOffset());
        }
    }

    @Test
    void advance_failure_rewinds_reader_without_ticking_counters(@TempDir Path dir)
            throws IOException {
        seedSamples(dir, 2);
        PluginMetrics metrics = new PluginMetrics();
        WalWriter writer = WalWriter.resume(dir, openActive(dir), 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
        Checkpoint checkpoint = spy(Checkpoint.loadOrCreate(dir));
        // Simulate disk-full / rename-fail on the next advance.
        org.mockito.Mockito.doThrow(new IOException("simulated checkpoint write failure"))
                .when(checkpoint).advance(org.mockito.ArgumentMatchers.anyLong());
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        when(http.write(any())).thenReturn(new WriteResult(WriteOutcome.SUCCESS, 200, 1, null));

        try (WalFlusher flusher = new WalFlusher(dir, writer, checkpoint, MAX_PAYLOAD,
                http, BATCH_SIZE, FLUSH_INTERVAL_MS, metrics)) {
            flusher.flushBatch(readOneBatch(dir, checkpoint));

            // The HTTP client returned 2xx so the samples were "delivered"
            // but the checkpoint refused to persist. samplesWritten MUST
            // stay at 0 — those samples will re-ship next cycle and we
            // must not double-count.
            assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue())
                    .isZero();
            // Pending counter cleared by the rewind.
            assertThat(flusher.pendingSampleCount()).isZero();
            // Checkpoint did not advance.
            assertThat(checkpoint.lastSentOffset()).isZero();
        }
    }

    @Test
    void dropped_4xx_ticks_counters_only_after_advance(@TempDir Path dir) throws IOException {
        seedSamples(dir, 2);
        PluginMetrics metrics = new PluginMetrics();
        WalWriter writer = WalWriter.resume(dir, openActive(dir), 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
        Checkpoint checkpoint = Checkpoint.loadOrCreate(dir);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        when(http.write(any())).thenReturn(new WriteResult(WriteOutcome.DROPPED_4XX, 400, 1, "bad"));

        try (WalFlusher flusher = new WalFlusher(dir, writer, checkpoint, MAX_PAYLOAD,
                http, BATCH_SIZE, FLUSH_INTERVAL_MS, metrics)) {
            flusher.flushBatch(readOneBatch(dir, checkpoint));

            // 4xx counters tick after the checkpoint advances past the
            // permanently-rejected batch.
            assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_DROPPED_4XX).longValue())
                    .isEqualTo(2);
            assertThat(metrics.snapshot().get(PluginMetrics.WAL_BATCHES_DROPPED_4XX).longValue())
                    .isEqualTo(1);
            // SAMPLES_WRITTEN does NOT tick on a 4xx.
            assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue())
                    .isZero();
            // Checkpoint advanced (4xx is permanent — match v0.4 drop).
            assertThat(checkpoint.lastSentOffset()).isEqualTo(writer.currentOffset());
        }
    }

    @Test
    void transport_error_rewinds_without_advancing_or_ticking(@TempDir Path dir)
            throws IOException {
        seedSamples(dir, 2);
        PluginMetrics metrics = new PluginMetrics();
        WalWriter writer = WalWriter.resume(dir, openActive(dir), 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
        Checkpoint checkpoint = Checkpoint.loadOrCreate(dir);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        when(http.write(any()))
                .thenReturn(new WriteResult(WriteOutcome.TRANSPORT_ERROR, 0, 5, "EOF"));

        try (WalFlusher flusher = new WalFlusher(dir, writer, checkpoint, MAX_PAYLOAD,
                http, BATCH_SIZE, FLUSH_INTERVAL_MS, metrics)) {
            flusher.flushBatch(readOneBatch(dir, checkpoint));

            // No advance, no counter ticks for written/4xx; pending
            // cleared by the rewind so a subsequent retry doesn't
            // double-count.
            assertThat(checkpoint.lastSentOffset()).isZero();
            assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue())
                    .isZero();
            assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_DROPPED_4XX).longValue())
                    .isZero();
            assertThat(flusher.pendingSampleCount()).isZero();
        }
    }

    @Test
    void corrupted_frames_skipped_pass_through_to_counter(@TempDir Path dir) throws IOException {
        seedSamples(dir, 1);
        PluginMetrics metrics = new PluginMetrics();
        WalWriter writer = WalWriter.resume(dir, openActive(dir), 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
        Checkpoint checkpoint = Checkpoint.loadOrCreate(dir);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        when(http.write(any())).thenReturn(new WriteResult(WriteOutcome.SUCCESS, 200, 1, null));

        try (WalFlusher flusher = new WalFlusher(dir, writer, checkpoint, MAX_PAYLOAD,
                http, BATCH_SIZE, FLUSH_INTERVAL_MS, metrics)) {
            // Synthesise a ReadResult with corruption-skip events. The
            // payload itself doesn't matter for the counter assertion
            // — flushBatch just inspects corruptedFramesSkipped on the
            // record.
            ReadResult realBatch = readOneBatch(dir, checkpoint);
            ReadResult withCorruption = new ReadResult(
                    realBatch.payloads(), realBatch.newOffset(), 3);
            flusher.flushBatch(withCorruption);

            assertThat(metrics.snapshot().get(PluginMetrics.WAL_FRAMES_DROPPED_CORRUPTED).longValue())
                    .isEqualTo(3);
        }
    }

    @Test
    void pending_sample_count_tracks_drain_and_clears_on_advance(@TempDir Path dir)
            throws IOException {
        seedSamples(dir, 5);
        PluginMetrics metrics = new PluginMetrics();
        WalWriter writer = WalWriter.resume(dir, openActive(dir), 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
        Checkpoint checkpoint = Checkpoint.loadOrCreate(dir);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        // First call returns 5xx (pending must NOT clear); second returns 2xx.
        when(http.write(any()))
                .thenReturn(new WriteResult(WriteOutcome.DROPPED_5XX_EXHAUSTED, 503, 5, "bad"))
                .thenReturn(new WriteResult(WriteOutcome.SUCCESS, 200, 1, null));

        try (WalFlusher flusher = new WalFlusher(dir, writer, checkpoint, MAX_PAYLOAD,
                http, BATCH_SIZE, FLUSH_INTERVAL_MS, metrics)) {
            // First flush: 5xx → rewind path clears pending counter
            // (since the rewound reader will re-deliver these samples
            // on the next cycle, leaving the pre-rewind count would
            // make multi-cycle outages over-report).
            flusher.flushBatch(readOneBatch(dir, checkpoint));
            assertThat(flusher.pendingSampleCount()).isZero();

            // Second flush after rewind: same 5 samples, this time
            // succeeds → checkpoint advances, pending stays at 0.
            flusher.flushBatch(readOneBatch(dir, checkpoint));
            assertThat(flusher.pendingSampleCount()).isZero();
            assertThat(checkpoint.lastSentOffset()).isEqualTo(writer.currentOffset());
            assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue())
                    .isEqualTo(5);
        }
    }

    @Test
    void stop_closes_reader_even_if_start_was_never_called(@TempDir Path dir) throws IOException {
        // Reproduces the rollback path: storage's startWalMode constructs
        // the flusher, then a downstream constructor throws → rollbackStart
        // calls flusher.stop(0) without flusher.start(). The reader is
        // eagerly created in the constructor; stop() must close it
        // regardless of whether the run-loop ever started.
        seedSamples(dir, 1);
        PluginMetrics metrics = new PluginMetrics();
        WalWriter writer = WalWriter.resume(dir, openActive(dir), 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD);
        Checkpoint checkpoint = Checkpoint.loadOrCreate(dir);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);

        WalFlusher flusher = new WalFlusher(dir, writer, checkpoint, MAX_PAYLOAD,
                http, BATCH_SIZE, FLUSH_INTERVAL_MS, metrics);
        // No start() — straight to stop. Should not throw, must close reader.
        flusher.stop(0);
        // Idempotent — second stop is a no-op.
        flusher.stop(0);
    }

    // --- helpers ----------------------------------------------------------------

    private static org.opennms.plugins.prometheus.remotewriter.wal.WalSegment openActive(Path dir)
            throws IOException {
        // Pick the newest segment (the one a WalRecovery would resume into).
        long start = java.util.Arrays.stream(dir.toFile().listFiles())
                .map(java.io.File::getName)
                .filter(n -> n.endsWith(".seg"))
                .mapToLong(n -> Long.parseLong(n.substring(0, n.length() - 4)))
                .max()
                .orElse(0L);
        Path segPath = dir.resolve(String.format("%020d.seg", start));
        return org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.openForAppend(
                segPath, start, FsyncPolicy.BATCH, MAX_PAYLOAD,
                /* sampleCountFromRecovery */ 0);
    }

    private static void seedSamples(Path dir, int count) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < count; i++) {
                Map<String, String> labels = new LinkedHashMap<>();
                labels.put("__name__", "test_metric");
                labels.put("instance", "ut-" + i);
                MappedSample s = new MappedSample(labels, 1_000L + i, (double) i);
                w.append(WalEntryCodec.encode(s));
            }
        }
    }

    /** Construct a fresh reader at the current checkpoint to drain a batch. */
    private static ReadResult readOneBatch(Path dir, Checkpoint checkpoint) throws IOException {
        try (org.opennms.plugins.prometheus.remotewriter.wal.WalReader r =
                new org.opennms.plugins.prometheus.remotewriter.wal.WalReader(
                        dir, checkpoint.lastSentOffset(), MAX_PAYLOAD)) {
            return r.nextBatch(BATCH_SIZE);
        }
    }
}
