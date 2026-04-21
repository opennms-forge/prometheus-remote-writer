/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/**
 * Plugin-internal metrics registry. Counters are owned by this class and
 * updated by the flush pipeline; gauges are registered with callbacks to the
 * components that already hold authoritative state (SampleQueue, HttpClient,
 * LabelMapper, Storage), so there is a single source of truth per metric.
 */
public final class PluginMetrics {

    public static final String SAMPLES_WRITTEN            = "samples_written_total";
    public static final String SAMPLES_DROPPED_4XX        = "samples_dropped_4xx_total";
    public static final String SAMPLES_DROPPED_5XX        = "samples_dropped_5xx_total";
    public static final String SAMPLES_DROPPED_TRANSPORT  = "samples_dropped_transport_total";
    public static final String SAMPLES_DROPPED_QUEUE_FULL = "samples_dropped_queue_full_total";
    public static final String SAMPLES_DROPPED_NONFINITE  = "samples_dropped_nonfinite_total";
    public static final String SAMPLES_DROPPED_DUPLICATE  = "samples_dropped_duplicate_total";
    public static final String DELETE_NOOP                = "delete_noop_total";
    public static final String METADATA_DENYLIST_BLOCKED  = "metadata_denylist_blocked_total";
    public static final String QUEUE_DEPTH                = "queue_depth";
    public static final String HTTP_BYTES_WRITTEN         = "http_bytes_written_total";
    public static final String HTTP_WRITES_SUCCESSFUL     = "http_writes_successful_total";
    public static final String HTTP_WRITES_FAILED         = "http_writes_failed_total";
    public static final String HTTP_IN_FLIGHT             = "http_in_flight";

    private final MetricRegistry registry = new MetricRegistry();
    private final Counter samplesWritten;
    private final Counter samplesDropped4xx;
    private final Counter samplesDropped5xx;
    private final Counter samplesDroppedTransport;
    private final Counter samplesDroppedNonfinite;
    private final Counter samplesDroppedDuplicate;

    public PluginMetrics() {
        this.samplesWritten          = registry.counter(SAMPLES_WRITTEN);
        this.samplesDropped4xx       = registry.counter(SAMPLES_DROPPED_4XX);
        this.samplesDropped5xx       = registry.counter(SAMPLES_DROPPED_5XX);
        this.samplesDroppedTransport = registry.counter(SAMPLES_DROPPED_TRANSPORT);
        this.samplesDroppedNonfinite = registry.counter(SAMPLES_DROPPED_NONFINITE);
        this.samplesDroppedDuplicate = registry.counter(SAMPLES_DROPPED_DUPLICATE);
    }

    public MetricRegistry registry() { return registry; }

    // ---- counter mutators (called by Flusher per flush) -------------------

    public void samplesWritten(long n)          { if (n > 0) samplesWritten.inc(n); }
    public void samplesDropped4xx(long n)       { if (n > 0) samplesDropped4xx.inc(n); }
    public void samplesDropped5xx(long n)       { if (n > 0) samplesDropped5xx.inc(n); }
    public void samplesDroppedTransport(long n) { if (n > 0) samplesDroppedTransport.inc(n); }
    public void samplesDroppedNonfinite(long n) { if (n > 0) samplesDroppedNonfinite.inc(n); }
    public void samplesDroppedDuplicate(long n) { if (n > 0) samplesDroppedDuplicate.inc(n); }

    // ---- gauge registration (called by Storage on start) ------------------

    public void registerLongGauge(String name, LongSupplier supplier) {
        // Replace an existing gauge with the same name (idempotent on hot-reload).
        if (registry.getGauges().containsKey(name)) {
            registry.remove(name);
        }
        Gauge<Long> gauge = supplier::getAsLong;
        registry.register(name, gauge);
    }

    // ---- read side (for StatsCommand) -------------------------------------

    /** Snapshot of all registered metrics as a name → Number map, sorted by name. */
    public Map<String, Number> snapshot() {
        Map<String, Number> out = new LinkedHashMap<>();
        registry.getCounters().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> out.put(e.getKey(), e.getValue().getCount()));
        registry.getGauges().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    Object v = e.getValue().getValue();
                    out.put(e.getKey(), v instanceof Number n ? n : 0);
                });
        return out;
    }
}
