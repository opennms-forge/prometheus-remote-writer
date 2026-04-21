/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.mapper.LabelMapper;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.queue.Flusher;
import org.opennms.plugins.prometheus.remotewriter.queue.SampleQueue;
import org.opennms.plugins.prometheus.remotewriter.read.PrometheusReadClient;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top-level TimeSeriesStorage bean — composes the full pipeline.
 *
 * <p>On {@link #start()} the bean validates config, builds a fresh set of
 * collaborators (LabelMapper, SampleQueue, RemoteWriteHttpClient,
 * PrometheusReadClient, Flusher) and registers Dropwizard gauges pointing
 * at the authoritative state on each.
 *
 * <p>{@link #store(List)} maps each OpenNMS {@link Sample} through the
 * {@link LabelMapper} and enqueues the result. A queue-full condition
 * throws {@link StorageException} — OpenNMS sees the backpressure signal.
 *
 * <p>On {@link #stop()} the bean stops accepting new enqueues, gives the
 * Flusher up to {@code shutdown.grace-period-ms} to drain, and then
 * shuts the HTTP clients down. Any samples still in the queue after the
 * grace window are logged at WARN.
 */
public class PrometheusRemoteWriterStorage implements TimeSeriesStorage {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusRemoteWriterStorage.class);
    private static final long DELETE_WARN_INTERVAL_MS = 60_000L;

    private static final AtomicReference<PrometheusRemoteWriterConfig> LAST_ACTIVE =
            new AtomicReference<>();

    private final PrometheusRemoteWriterConfig config;

    // Populated on start()
    private volatile LabelMapper           labelMapper;
    private volatile SampleQueue           queue;
    private volatile RemoteWriteHttpClient writeClient;
    private volatile PrometheusReadClient  readClient;
    private volatile Flusher               flusher;
    private volatile PluginMetrics         metrics;
    private volatile boolean               acceptingWrites;

    private final AtomicLong deleteNoopTotal        = new AtomicLong();
    private final AtomicLong deleteWarnLastMs       = new AtomicLong();
    private final AtomicLong deleteWarnSinceLastLog = new AtomicLong();

    public PrometheusRemoteWriterStorage(PrometheusRemoteWriterConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    // --- Blueprint lifecycle ------------------------------------------------

    public void start() {
        try {
            config.validate();
        } catch (IllegalStateException bad) {
            LOG.error("Refusing to activate prometheus-remote-writer: {}", bad.getMessage());
            throw bad;
        }

        logActivationOrDiff();

        this.metrics     = new PluginMetrics();
        this.labelMapper = new LabelMapper(config);
        this.queue       = new SampleQueue(config.getQueueCapacity());
        this.writeClient = new RemoteWriteHttpClient(config);
        this.readClient  = new PrometheusReadClient(config);
        this.flusher     = new Flusher(queue, writeClient,
                                       config.getBatchSize(),
                                       config.getFlushIntervalMs(),
                                       metrics);
        registerGauges();
        this.flusher.start();
        this.acceptingWrites = true;
    }

    public void stop() {
        LOG.info("prometheus-remote-writer stopping");
        this.acceptingWrites = false;
        if (flusher != null) {
            flusher.stop(config.getShutdownGracePeriodMs());
            int residual = queue != null ? queue.depth() : 0;
            if (residual > 0) {
                LOG.warn("shutdown completed with {} sample(s) still queued; dropping", residual);
            }
            flusher = null;
        }
        if (writeClient != null) { writeClient.shutdown(); writeClient = null; }
        if (readClient  != null) { readClient.shutdown();  readClient  = null; }
        labelMapper = null;
        queue       = null;
        metrics     = null;
    }

    // --- TimeSeriesStorage -------------------------------------------------

    @Override
    public void store(List<Sample> samples) throws StorageException {
        if (!acceptingWrites) {
            throw new StorageException("prometheus-remote-writer is not accepting writes "
                    + "(plugin is stopped or not yet started)");
        }
        if (samples == null || samples.isEmpty()) return;

        for (Sample s : samples) {
            MappedSample mapped = labelMapper.map(s);
            if (mapped == null) continue; // e.g. sample with no name
            queue.enqueue(mapped);
        }
    }

    @Override
    public List<Metric> findMetrics(Collection<TagMatcher> tagMatchers) throws StorageException {
        PrometheusReadClient rc = readClient;
        if (rc == null) throw new StorageException("findMetrics called before start()");
        return rc.findMetrics(tagMatchers);
    }

    @Override
    public List<Sample> getTimeseries(TimeSeriesFetchRequest request) throws StorageException {
        // Adapter for the deprecated SPI method; delegates to getTimeSeriesData.
        TimeSeriesData data = getTimeSeriesData(request);
        Metric metric = data.getMetric();
        List<Sample> out = new ArrayList<>(data.getDataPoints().size());
        for (DataPoint dp : data.getDataPoints()) {
            out.add(ImmutableSample.builder()
                    .metric(metric)
                    .time(dp.getTime())
                    .value(dp.getValue())
                    .build());
        }
        return out;
    }

    @Override
    public TimeSeriesData getTimeSeriesData(TimeSeriesFetchRequest request) throws StorageException {
        PrometheusReadClient rc = readClient;
        if (rc == null) throw new StorageException("getTimeSeriesData called before start()");
        return rc.getTimeSeriesData(request);
    }

    @Override
    public boolean supportsAggregation(Aggregation aggregation) {
        return aggregation == Aggregation.NONE;
    }

    @Override
    public void delete(Metric metric) {
        deleteNoopTotal.incrementAndGet();
        deleteWarnSinceLastLog.incrementAndGet();

        long now = System.currentTimeMillis();
        long prev = deleteWarnLastMs.get();
        if (now - prev >= DELETE_WARN_INTERVAL_MS
                && deleteWarnLastMs.compareAndSet(prev, now)) {
            long count = deleteWarnSinceLastLog.getAndSet(0);
            LOG.warn("delete(Metric) called {} time(s) in the last {}s — the plugin "
                   + "does not propagate deletes to Prometheus (no remote-write delete "
                   + "semantic exists). Configure retention at the backend tier.",
                    count, DELETE_WARN_INTERVAL_MS / 1000);
        }
    }

    // --- Accessors for the Karaf shell command ----------------------------

    public PluginMetrics getMetrics() { return metrics; }
    public long getDeleteNoopTotal()  { return deleteNoopTotal.get(); }

    // --- internals --------------------------------------------------------

    private void logActivationOrDiff() {
        PrometheusRemoteWriterConfig previous = LAST_ACTIVE.getAndSet(config);
        if (previous == null) {
            LOG.info("prometheus-remote-writer activated (write.url={}, read.url={})",
                     config.getWriteUrl(), config.getReadUrl());
            return;
        }
        List<String> changes = config.diff(previous);
        if (changes.isEmpty()) {
            LOG.info("prometheus-remote-writer reloaded; configuration unchanged");
        } else {
            LOG.info("prometheus-remote-writer reloaded; {} change(s):", changes.size());
            for (String line : changes) {
                LOG.info("  {}", line);
            }
        }
    }

    private void registerGauges() {
        metrics.registerLongGauge(PluginMetrics.QUEUE_DEPTH,            () -> (long) queue.depth());
        metrics.registerLongGauge(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL,
                                  queue::getSamplesDroppedQueueFull);
        metrics.registerLongGauge(PluginMetrics.HTTP_BYTES_WRITTEN,     writeClient::getBytesWritten);
        metrics.registerLongGauge(PluginMetrics.HTTP_WRITES_SUCCESSFUL, writeClient::getWritesSuccessful);
        metrics.registerLongGauge(PluginMetrics.HTTP_WRITES_FAILED,
                () -> writeClient.getWrites4xx()
                    + writeClient.getWrites5xxExhausted()
                    + writeClient.getWritesTransportError());
        metrics.registerLongGauge(PluginMetrics.DELETE_NOOP,            this::getDeleteNoopTotal);
        metrics.registerLongGauge(PluginMetrics.METADATA_DENYLIST_BLOCKED,
                                  labelMapper::getMetadataDenylistBlockedCount);
    }
}
