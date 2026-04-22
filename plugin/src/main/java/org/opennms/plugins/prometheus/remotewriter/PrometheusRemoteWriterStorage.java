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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>The active pipeline lives in an immutable {@link Active} record published
 * via a single {@code volatile} reference. {@link #start()} atomically
 * publishes a new Active after constructing every collaborator; {@link #stop()}
 * atomically clears the reference before tearing the collaborators down.
 * SPI methods snapshot the reference once at entry and operate on that local
 * snapshot — this eliminates the torn-read race between a reader (e.g.
 * {@code store()}) and {@code stop()}.
 */
public class PrometheusRemoteWriterStorage implements TimeSeriesStorage {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusRemoteWriterStorage.class);
    private static final long DELETE_WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

    /** Previous active config, used only for hot-reload diff logging. */
    private static final AtomicReference<PrometheusRemoteWriterConfig> LAST_ACTIVE =
            new AtomicReference<>();

    /**
     * One-shot gate for the instance.id-unset WARN. Static so the warning
     * fires once per bundle lifecycle regardless of how many times the
     * blueprint container reloads the plugin on config changes.
     */
    private static final AtomicBoolean INSTANCE_ID_UNSET_WARNED = new AtomicBoolean(false);

    /**
     * Everything constructed at {@link #start()} time. Published as a unit via
     * the {@link #active} volatile so SPI callers can snapshot the whole
     * pipeline without worrying about partial views.
     */
    private record Active(
            LabelMapper           labelMapper,
            SampleQueue           queue,
            RemoteWriteHttpClient writeClient,
            PrometheusReadClient  readClient,
            Flusher               flusher,
            PluginMetrics         metrics) {}

    private final PrometheusRemoteWriterConfig config;
    private volatile Active active;

    private final AtomicLong deleteNoopTotal        = new AtomicLong();
    // nanoTime-based so NTP backsteps or container resume can't freeze the
    // throttle in the "just logged" state forever.
    private final AtomicLong deleteWarnLastNanos    = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong deleteWarnSinceLastLog = new AtomicLong();

    public PrometheusRemoteWriterStorage(PrometheusRemoteWriterConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    // --- Blueprint lifecycle -----------------------------------------------

    /** Idempotent: if already active, does nothing. On construction failure,
     *  rolls back any collaborators that were already built.
     *  <p>
     *  Missing / invalid config does NOT throw from here. When this bundle
     *  is deployed as a KAR, Karaf starts the blueprint container before
     *  Felix fileinstall has pushed the etc/ cfg file into ConfigAdmin, so
     *  the cm:property-placeholder falls back to its empty defaults and
     *  validate() would fail. Throwing would mark the container failed
     *  permanently — the update-strategy="reload" on the placeholder
     *  cannot revive it. Instead we log a warning and leave {@code active}
     *  null; the SPI methods already reject calls in that state. Once
     *  ConfigAdmin delivers real properties the placeholder reload tears
     *  down and re-creates the container, giving us another shot.
     */
    public synchronized void start() {
        if (active != null) {
            LOG.debug("start() called while already active; ignoring");
            return;
        }
        try {
            config.validate();
        } catch (IllegalStateException bad) {
            LOG.warn("prometheus-remote-writer not yet configured ({}); "
                   + "waiting for ConfigAdmin to deliver real properties. "
                   + "If this persists, check etc/org.opennms.plugins.tss.prometheusremotewriter.cfg.",
                    bad.getMessage());
            return;
        }

        warnIfInstanceIdUnset();

        PluginMetrics         m  = null;
        LabelMapper           lm = null;
        SampleQueue           q  = null;
        RemoteWriteHttpClient wc = null;
        PrometheusReadClient  rc = null;
        Flusher               f  = null;
        try {
            m  = new PluginMetrics();
            lm = new LabelMapper(config);
            q  = new SampleQueue(config.getQueueCapacity());
            wc = new RemoteWriteHttpClient(config);
            rc = new PrometheusReadClient(config);
            f  = new Flusher(q, wc, config.getBatchSize(), config.getFlushIntervalMs(), m);

            Active built = new Active(lm, q, wc, rc, f, m);
            registerGauges(built);
            logActivationOrDiff();
            f.start();
            // Publish last — readers that see a non-null active are guaranteed
            // to see every collaborator fully constructed.
            active = built;
        } catch (RuntimeException e) {
            // Roll back in reverse construction order. Swallow secondary
            // failures so the original exception surfaces.
            rollbackStart(f, wc, rc);
            throw e;
        }
    }

    public synchronized void stop() {
        Active a = active;
        if (a == null) {
            LOG.debug("stop() called while not active; ignoring");
            return;
        }
        // Clear first so concurrent SPI callers see the state change
        // immediately and fail cleanly with StorageException instead of
        // touching collaborators being torn down.
        active = null;

        LOG.info("prometheus-remote-writer stopping");
        try {
            a.flusher().stop(config.getShutdownGracePeriodMs());
            int residual = a.queue().depth();
            if (residual > 0) {
                LOG.warn("shutdown completed with {} sample(s) still queued; dropping", residual);
            }
        } catch (RuntimeException e) {
            LOG.warn("error stopping flusher: {}", e.getMessage(), e);
        }
        try { a.writeClient().shutdown(); } catch (RuntimeException e) { LOG.warn("write client shutdown: {}", e.getMessage(), e); }
        try { a.readClient().shutdown();  } catch (RuntimeException e) { LOG.warn("read client shutdown: {}",  e.getMessage(), e); }

        // Clear the static hot-reload diff anchor so a fresh start() after
        // stop() logs "activated" rather than a spurious "reloaded".
        LAST_ACTIVE.set(null);
    }

    // --- TimeSeriesStorage -------------------------------------------------

    @Override
    public void store(List<Sample> samples) throws StorageException {
        Active a = active;
        if (a == null) {
            throw new StorageException("prometheus-remote-writer is not accepting writes "
                    + "(plugin is stopped or not yet started)");
        }
        if (samples == null || samples.isEmpty()) return;

        for (Sample s : samples) {
            MappedSample mapped = a.labelMapper().map(s);
            if (mapped == null) continue;
            a.queue().enqueue(mapped);
        }
    }

    @Override
    public List<Metric> findMetrics(Collection<TagMatcher> tagMatchers) throws StorageException {
        Active a = active;
        if (a == null) throw new StorageException("findMetrics called before start()");
        return a.readClient().findMetrics(tagMatchers);
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
        Active a = active;
        if (a == null) throw new StorageException("getTimeSeriesData called before start()");
        return a.readClient().getTimeSeriesData(request);
    }

    @Override
    public boolean supportsAggregation(Aggregation aggregation) {
        return aggregation == Aggregation.NONE;
    }

    @Override
    public void delete(Metric metric) {
        deleteNoopTotal.incrementAndGet();
        deleteWarnSinceLastLog.incrementAndGet();

        long now = System.nanoTime();
        long prev = deleteWarnLastNanos.get();
        // First call: prev == Long.MIN_VALUE; subtraction would overflow, so
        // special-case to "log the first one".
        boolean due = prev == Long.MIN_VALUE || (now - prev) >= DELETE_WARN_INTERVAL_NANOS;
        if (due && deleteWarnLastNanos.compareAndSet(prev, now)) {
            long count = deleteWarnSinceLastLog.getAndSet(0);
            LOG.warn("delete(Metric) called {} time(s) in the last {}s — the plugin "
                   + "does not propagate deletes to Prometheus (no remote-write delete "
                   + "semantic exists). Configure retention at the backend tier.",
                    count, TimeUnit.NANOSECONDS.toSeconds(DELETE_WARN_INTERVAL_NANOS));
        }
    }

    // --- Accessors for the Karaf shell command -----------------------------

    public PluginMetrics getMetrics() {
        Active a = active;
        return a == null ? null : a.metrics();
    }

    public long getDeleteNoopTotal() { return deleteNoopTotal.get(); }

    // --- internals ---------------------------------------------------------

    private void warnIfInstanceIdUnset() {
        String iid = config.getInstanceId();
        if ((iid == null || iid.isEmpty())
                && INSTANCE_ID_UNSET_WARNED.compareAndSet(false, true)) {
            LOG.warn("instance.id is not set. This is fine for a single OpenNMS "
                   + "instance writing to a dedicated backend. If you run multiple "
                   + "OpenNMS instances against a shared Prometheus-compatible "
                   + "backend, set instance.id to a stable per-instance identifier "
                   + "so samples can be distinguished by the onms_instance_id label.");
        }
    }

    /** Visible for tests — resets the one-shot WARN gate so a sequence of
     *  start()/stop() cycles within a single test run can exercise the WARN
     *  deterministically. */
    static void resetInstanceIdWarnedForTesting() {
        INSTANCE_ID_UNSET_WARNED.set(false);
    }

    /** Visible for tests — true once the WARN gate has flipped (i.e. the
     *  LOG.warn in {@link #warnIfInstanceIdUnset()} has been emitted once
     *  already within this JVM). Lets tests assert the one-shot semantic
     *  without taking a dependency on a log-capture framework. */
    static boolean isInstanceIdWarnedForTesting() {
        return INSTANCE_ID_UNSET_WARNED.get();
    }

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

    private void registerGauges(Active a) {
        PluginMetrics m = a.metrics();
        // Every lambda below captures the Active parameter, so gauges continue
        // reading the correct collaborators even if stop() has cleared the
        // volatile `active`. The Active object itself stays alive as long as
        // the registry holds these gauges.
        m.registerLongGauge(PluginMetrics.QUEUE_DEPTH,              () -> (long) a.queue().depth());
        m.registerLongGauge(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL, a.queue()::getSamplesDroppedQueueFull);
        m.registerLongGauge(PluginMetrics.HTTP_BYTES_WRITTEN,       a.writeClient()::getBytesWritten);
        m.registerLongGauge(PluginMetrics.HTTP_WRITES_SUCCESSFUL,   a.writeClient()::getWritesSuccessful);
        m.registerLongGauge(PluginMetrics.HTTP_WRITES_FAILED,
                () -> a.writeClient().getWrites4xx()
                    + a.writeClient().getWrites5xxExhausted()
                    + a.writeClient().getWritesTransportError());
        m.registerLongGauge(PluginMetrics.HTTP_IN_FLIGHT,           () -> (long) a.writeClient().getInFlightCalls());
        m.registerLongGauge(PluginMetrics.DELETE_NOOP,              this::getDeleteNoopTotal);
        m.registerLongGauge(PluginMetrics.METADATA_DENYLIST_BLOCKED, a.labelMapper()::getMetadataDenylistBlockedCount);
    }

    private static void rollbackStart(Flusher f, RemoteWriteHttpClient wc, PrometheusReadClient rc) {
        if (f != null) {
            try { f.stop(0); } catch (RuntimeException ignored) {}
        }
        if (wc != null) {
            try { wc.shutdown(); } catch (RuntimeException ignored) {}
        }
        if (rc != null) {
            try { rc.shutdown(); } catch (RuntimeException ignored) {}
        }
    }
}
