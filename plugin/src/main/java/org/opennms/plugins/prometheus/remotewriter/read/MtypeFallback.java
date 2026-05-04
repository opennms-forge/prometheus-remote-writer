/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defensive read-time fallback: when a Metric reconstructed from a Prometheus
 * response lacks an {@code mtype} meta tag, synthesize {@code mtype="gauge"}.
 *
 * <p>Background: OpenNMS-core's
 * {@code NewtsConverterUtils.dataPointToRow} unconditionally dereferences
 * {@code metric.getFirstTagByKey(MetaTagNames.mtype).getValue()} when
 * converting plugin-returned Samples to Newts rows for graph rendering.
 * Without an {@code mtype} meta tag, every graph fetch trips a {@code NullPointerException}
 * and returns HTTP 500.
 *
 * <p>The plugin's write side now emits an {@code mtype} Prometheus label for
 * every sample whose source carries the meta tag (see {@code LabelMapper}),
 * so post-fix data round-trips correctly. This fallback exists for data
 * already in Prometheus from before the fix — graphs render as gauges
 * (cumulative values for counters; visibly less informative but never
 * mathematically wrong).
 *
 * <p>Why {@code "gauge"} and not {@code "count"}? {@code NewtsConverterUtils.toNewts}
 * only tolerates {@code Metric.Mtype.{count, gauge}} — anything else throws
 * {@code IllegalArgumentException}. Counter rendered as a gauge shows
 * cumulative values (less informative, not wrong); gauge rendered as a
 * counter would invoke late-aggregation derivative computation and produce
 * nonsensical rates. Gauge is the safe default.
 *
 * <p>Each synthesis increments the {@code samples_synthesized_mtype_total}
 * plugin counter. Operators monitor this counter to know when their
 * Prometheus retention has aged out the pre-fix data — once it stops rising,
 * every rendered graph uses authentic mtype values from the writer.
 *
 * <p>A WARN is logged exactly once per distinct {@code __name__} value, gated
 * by an insertion-ordered LRU bounded at {@link #WARN_TRACKING_CAP} entries.
 * Beyond that cap the counter still increments but no further WARNs fire for
 * the evicted names — bounding log spam under pathological metric-name
 * cardinality.
 */
public final class MtypeFallback {

    /** Cap on the WARN-tracking set. Beyond this many distinct metric names,
     *  the eldest warned-about name is evicted from the set; subsequent hits
     *  on the evicted name will WARN again (which is acceptable, given that
     *  256 distinct metric names is already pathological). */
    static final int WARN_TRACKING_CAP = 256;

    /** The synthetic value injected on missing {@code mtype}. {@code "gauge"}
     *  is the only safe choice — {@code NewtsConverterUtils.toNewts} only
     *  accepts {@code count} or {@code gauge} from the {@code Metric.Mtype}
     *  enum, and gauging a counter is less wrong than counting a gauge. */
    static final String SYNTHETIC_MTYPE = "gauge";

    private static final Logger LOG = LoggerFactory.getLogger(MtypeFallback.class);

    private final PluginMetrics metrics;
    /** Insertion-ordered, bounded by {@link #WARN_TRACKING_CAP} via
     *  {@code removeEldestEntry}. Synchronized externally on each access so
     *  the put-then-warn pair is atomic; LinkedHashMap is not thread-safe. */
    private final Map<String, Boolean> warnedMetrics;

    public MtypeFallback(PluginMetrics metrics) {
        // metrics MAY be null in tests that don't care about the counter.
        this.metrics = metrics;
        this.warnedMetrics = Collections.synchronizedMap(
                new LinkedHashMap<String, Boolean>(WARN_TRACKING_CAP + 1, 0.75f, false) {
                    private static final long serialVersionUID = 1L;
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                        return size() > WARN_TRACKING_CAP;
                    }
                });
    }

    /**
     * Return a Metric guaranteed to carry an {@code mtype} meta tag. When the
     * input already has one, returned unchanged (zero allocations).
     *
     * <p>Both call sites in {@link PrometheusReadClient} pass non-null Metrics
     * (the parser never returns null; the request Metric is
     * {@link java.util.Objects#requireNonNull}-checked upstream), so this
     * method does not guard {@code input == null} — a null input is a caller
     * bug, not a fallback case.
     */
    public Metric apply(Metric input) {
        Tag existing = input.getFirstTagByKey(MetaTagNames.mtype);
        if (existing != null && existing.getValue() != null && !existing.getValue().isEmpty()) {
            return input;
        }
        if (metrics != null) {
            metrics.samplesSynthesizedMtype(1);
        }
        String metricName = nameOf(input);
        // Synchronize externally: the put-then-warn pair must be atomic to
        // guarantee "WARN fires exactly once per metric name". Synchronized
        // map alone only locks individual operations.
        boolean firstTime;
        synchronized (warnedMetrics) {
            firstTime = warnedMetrics.put(metricName, Boolean.TRUE) == null;
        }
        if (firstTime) {
            LOG.warn("Synthesizing mtype=\"{}\" for metric '{}' (no mtype label in "
                    + "Prometheus response — pre-fix data?). Subsequent occurrences "
                    + "for this metric will be silent.",
                    SYNTHETIC_MTYPE, metricName);
        }
        return withSyntheticMtype(input);
    }

    /** Visible for tests — unmodifiable snapshot of the WARN-tracking set,
     *  for asserting one-shot semantics and LRU eviction without depending
     *  on a logging framework. */
    Set<String> warnedMetricsForTesting() {
        synchronized (warnedMetrics) {
            return Set.copyOf(warnedMetrics.keySet());
        }
    }

    private static String nameOf(Metric m) {
        Tag t = m.getFirstTagByKey(org.opennms.integration.api.v1.timeseries.IntrinsicTagNames.name);
        return (t != null && t.getValue() != null) ? t.getValue() : "<unnamed>";
    }

    /** Rebuild the Metric preserving every existing tag and appending
     *  {@code mtype=gauge} as a meta tag. Matches the partition convention
     *  used by {@code PromResponseParser.labelObjectToMetric}. Any existing
     *  empty-valued {@code mtype} meta tag is dropped during the copy so the
     *  synthetic value isn't shadowed by an empty predecessor. */
    private static Metric withSyntheticMtype(Metric input) {
        ImmutableMetric.MetricBuilder mb = ImmutableMetric.builder();
        for (Tag t : input.getIntrinsicTags()) mb.intrinsicTag(t.getKey(), t.getValue());
        for (Tag t : input.getMetaTags()) {
            // Drop a pre-existing empty mtype meta tag — we synthesize a real
            // value just below; copying the empty one through would either
            // shadow our addition (depending on the collection's iteration
            // semantics) or just leave a dangling empty entry.
            if (MetaTagNames.mtype.equals(t.getKey())) continue;
            mb.metaTag(t.getKey(), t.getValue());
        }
        for (Tag t : input.getExternalTags())  mb.externalTag(t.getKey(), t.getValue());
        mb.metaTag(MetaTagNames.mtype, SYNTHETIC_MTYPE);
        return mb.build();
    }
}
