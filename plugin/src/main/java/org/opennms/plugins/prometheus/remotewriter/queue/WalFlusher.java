/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteOutcome;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteResult;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wal.Checkpoint;
import org.opennms.plugins.prometheus.remotewriter.wal.WalEntryCodec;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader.ReadResult;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background flush thread for the WAL-enabled path. Drains up to
 * {@code batch.size} payloads from a {@link WalReader}, decodes them via
 * {@link WalEntryCodec}, builds a snappy-compressed RW v1 payload, and
 * POSTs through {@link RemoteWriteHttpClient}.
 *
 * <p>Outcome handling:
 * <ul>
 *   <li>SUCCESS → advance the {@link Checkpoint} past the batch, advance
 *       the {@link WalWriter}'s reader-floor so the next eviction cycle
 *       can reclaim the shipped bytes, and run a GC pass to delete
 *       fully-shipped segments.</li>
 *   <li>DROPPED_4XX → advance just like SUCCESS (matches v0.4 "4xx
 *       drops the batch permanently" semantics). The
 *       {@code wal_batches_dropped_4xx_total} counter ticks alongside
 *       the existing {@code samples_dropped_4xx_total}.</li>
 *   <li>DROPPED_5XX_EXHAUSTED / TRANSPORT_ERROR → checkpoint stays put.
 *       The reader is reset to {@code checkpoint.lastSentOffset}, so the
 *       next cycle re-reads and retries the same batch. The WAL holds
 *       the data durably across any outage length until the endpoint
 *       recovers or the cap is reached and overflow policy kicks in.</li>
 * </ul>
 *
 * <p>Unlike {@link Flusher}, shutdown does NOT attempt to drain residual
 * samples — the WAL is durable, so anything not shipped at stop()
 * replays on next start. The grace window controls only the in-flight
 * HTTP request deadline.
 */
public final class WalFlusher {

    private static final Logger LOG = LoggerFactory.getLogger(WalFlusher.class);

    private final Path walDir;
    private final WalWriter writer;
    private final Checkpoint checkpoint;
    private final int maxPayload;
    private final RemoteWriteHttpClient httpClient;
    private final int batchSize;
    private final long flushIntervalMs;
    private final PluginMetrics metrics;

    private WalReader reader;
    private volatile boolean running;
    private Thread thread;

    public WalFlusher(Path walDir, WalWriter writer, Checkpoint checkpoint,
                      int maxPayload, RemoteWriteHttpClient httpClient,
                      int batchSize, long flushIntervalMs, PluginMetrics metrics) {
        this.walDir          = Objects.requireNonNull(walDir);
        this.writer          = Objects.requireNonNull(writer);
        this.checkpoint      = Objects.requireNonNull(checkpoint);
        this.httpClient      = Objects.requireNonNull(httpClient);
        this.metrics         = Objects.requireNonNull(metrics);
        if (batchSize < 1)       throw new IllegalArgumentException("batchSize must be >= 1");
        if (flushIntervalMs < 1) throw new IllegalArgumentException("flushIntervalMs must be >= 1");
        this.maxPayload      = maxPayload;
        this.batchSize       = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.reader = new WalReader(walDir, checkpoint.lastSentOffset(), maxPayload);
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        thread = new Thread(this::run, "prometheus-remote-writer-wal-flusher");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Signal the flush loop to stop and wait up to {@code graceMs} for
     * any in-flight HTTP POST to complete. No WAL drain on shutdown —
     * anything unfinished is durable on disk and replays on next start.
     */
    public synchronized void stop(long graceMs) {
        if (!running) return;
        running = false;
        Thread t = thread;
        if (t == null) return;
        try {
            t.join(Math.max(1, graceMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            LOG.warn("wal-flusher did not stop within {}ms, interrupting", graceMs);
            t.interrupt();
            try {
                t.join(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        thread = null;
        try {
            reader.close();
        } catch (IOException e) {
            LOG.debug("wal-reader close: {}", e.getMessage());
        }
    }

    /** Current reader offset (for wal_oldest_sample_ts_ms gauge + shutdown logging). */
    public long readerOffset() {
        return reader.currentOffset();
    }

    private void run() {
        LOG.info("wal-flusher started (batchSize={}, flushIntervalMs={}, resumeOffset={})",
                batchSize, flushIntervalMs, checkpoint.lastSentOffset());
        while (running) {
            try {
                ReadResult batch = reader.nextBatch(batchSize);
                if (batch.isEmpty()) {
                    Thread.sleep(flushIntervalMs);
                    continue;
                }
                flushBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception unexpected) {
                LOG.error("wal-flusher caught unexpected exception", unexpected);
            }
        }
        LOG.info("wal-flusher stopped");
    }

    /** Package-private for unit tests — runs one flush iteration synchronously. */
    void flushBatch(ReadResult batch) {
        // Pass through any corruption-skip count from the reader so the
        // operator sees a counter tick (the reader logs WARN; this
        // surfaces it as a metric).
        if (batch.corruptedFramesSkipped() > 0) {
            metrics.walFramesDroppedCorrupted(batch.corruptedFramesSkipped());
        }

        // Decode each WAL-framed payload into a MappedSample. Decode
        // failures are surfaced as hard errors; the codec already
        // rejects malformed payloads with an actionable message, so
        // reaching here means the WAL is corrupted beyond what CRC +
        // schema validation can catch.
        List<MappedSample> samples = new ArrayList<>(batch.payloads().size());
        for (byte[] payload : batch.payloads()) {
            samples.add(WalEntryCodec.decode(payload));
        }

        BuildResult built = RemoteWriteRequestBuilder.build(samples);
        metrics.samplesDroppedNonfinite(built.samplesDroppedNonfinite());
        metrics.samplesDroppedDuplicate(built.samplesDroppedDuplicate());
        if (!built.hasContent()) {
            // No payload to POST (every sample was dropped as non-finite
            // / duplicate). Still advance the checkpoint — the samples
            // were WAL-durable and are now accounted for.
            advanceAfterBatch(batch.newOffset());
            return;
        }

        WriteResult result = httpClient.write(built.compressedPayload());
        switch (result.outcome()) {
            case SUCCESS -> {
                metrics.samplesWritten(built.samplesWritten());
                LOG.debug("wal-flushed {} samples in {} bytes on attempt {}",
                        built.samplesWritten(), built.compressedPayload().length, result.attemptsMade());
                advanceAfterBatch(batch.newOffset());
            }
            case DROPPED_4XX -> {
                metrics.samplesDropped4xx(built.samplesWritten());
                metrics.walBatchesDropped4xx(1);
                LOG.warn("wal-dropped batch of {} samples after 4xx: status={}",
                        built.samplesWritten(), result.httpStatus());
                advanceAfterBatch(batch.newOffset());
            }
            case DROPPED_5XX_EXHAUSTED, TRANSPORT_ERROR -> {
                if (result.outcome() == WriteOutcome.DROPPED_5XX_EXHAUSTED) {
                    LOG.warn("wal-retry pending for batch of {} samples after {} attempts: "
                            + "status={} — WAL holds the data; will retry next cycle",
                            built.samplesWritten(), result.attemptsMade(), result.httpStatus());
                } else {
                    LOG.warn("wal-retry pending for batch of {} samples after transport error: "
                            + "{} — WAL holds the data; will retry next cycle",
                            built.samplesWritten(), result.detail());
                }
                // Reset the reader to the checkpoint so the next cycle
                // re-reads the same bytes. Close the old reader's channel.
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.debug("wal-reader close during retry-reset: {}", e.getMessage());
                }
                reader = new WalReader(walDir, checkpoint.lastSentOffset(), maxPayload);
            }
        }
    }

    /**
     * Checkpoint + GC + reader-floor advance after a SUCCESS or 4xx
     * outcome. The on-disk checkpoint is the source of truth — if its
     * advance fails (e.g., disk full on checkpoint.json.tmp), reset the
     * reader so the next cycle re-reads and retries the same batch
     * rather than silently skipping it.
     *
     * <p>Reader-floor and GC are idempotent against an already-advanced
     * checkpoint; a crash between any two steps reconciles on next
     * startup.
     */
    private void advanceAfterBatch(long newOffset) {
        long previousOffset = checkpoint.lastSentOffset();
        try {
            checkpoint.advance(newOffset);
        } catch (IOException e) {
            // Checkpoint failed to persist. The reader has already moved
            // past these samples in-memory (nextBatch advanced it). If
            // we ignore the failure, the batch is silently lost from the
            // pipeline's POV (counted as "written" but checkpoint
            // disagrees). Reset the reader to the last-good checkpoint
            // so the next cycle re-reads and retries.
            LOG.error("wal-checkpoint advance failed; resetting reader to "
                    + "last-good offset {} for retry", previousOffset, e);
            try {
                reader.close();
            } catch (IOException close) {
                LOG.debug("wal-reader close during advance-fail reset: {}",
                        close.getMessage());
            }
            reader = new WalReader(walDir, previousOffset, maxPayload);
            return;
        }

        // Advance succeeded. Now best-effort: reader-floor and GC.
        // Failures here only delay GC / floor updates; next successful
        // batch re-runs them.
        try {
            writer.setReaderOffsetFloor(newOffset);
            long bytesPastCheckpoint = newOffset - previousOffset;
            if (bytesPastCheckpoint > 0) {
                metrics.walBytesCheckpointed(bytesPastCheckpoint);
            }
            long reclaimed = Checkpoint.gcSegments(walDir, newOffset);
            if (reclaimed > 0) {
                LOG.debug("wal-gc reclaimed {} bytes at checkpoint {}", reclaimed, newOffset);
            }
        } catch (IOException e) {
            LOG.warn("wal-gc failed (non-fatal — checkpoint advanced cleanly)", e);
        }
    }
}
