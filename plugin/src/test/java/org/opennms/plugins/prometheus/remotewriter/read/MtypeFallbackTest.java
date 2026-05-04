/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

class MtypeFallbackTest {

    @Test
    void synthesizes_gauge_when_no_mtype_meta_tag() {
        PluginMetrics metrics = new PluginMetrics();
        MtypeFallback fallback = new MtypeFallback(metrics);

        Metric input = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "ifHCInOctets")
                .intrinsicTag(IntrinsicTagNames.resourceId, "node[1].interfaceSnmp[eth0]")
                .build();

        Metric out = fallback.apply(input);

        assertThat(out.getFirstTagByKey(MetaTagNames.mtype).getValue()).isEqualTo("gauge");
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_SYNTHESIZED_MTYPE).longValue())
                .isEqualTo(1);
    }

    @Test
    void synthesized_mtype_lives_in_meta_tags_partition() {
        // getFirstTagByKey searches every partition, so the prior assertion
        // would pass even if synthesis put `mtype` in intrinsic by mistake.
        // OpenNMS-core treats mtype as a meta tag (MetaTagNames.mtype lives
        // in the meta partition by convention); pin the partition explicitly
        // so a future refactor that swaps `metaTag(...)` for
        // `intrinsicTag(...)` shows up as a named test failure.
        MtypeFallback fallback = new MtypeFallback(new PluginMetrics());
        Metric input = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "x")
                .build();

        Metric out = fallback.apply(input);

        assertThat(out.getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo(MetaTagNames.mtype);
                    assertThat(t.getValue()).isEqualTo("gauge");
                });
        // And NOT in intrinsic — the partition matters for downstream
        // consumers that walk getMetaTags() vs getIntrinsicTags() directly.
        assertThat(out.getIntrinsicTags())
                .extracting("key")
                .doesNotContain(MetaTagNames.mtype);
    }

    @Test
    void preserves_existing_intrinsic_and_meta_tags_when_synthesizing() {
        MtypeFallback fallback = new MtypeFallback(new PluginMetrics());
        Metric input = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "x")
                .intrinsicTag(IntrinsicTagNames.resourceId, "node[1].nodeSnmp[]")
                .metaTag("node", "1:1")
                .metaTag("if_name", "eth0")
                .build();

        Metric out = fallback.apply(input);

        assertThat(out.getFirstTagByKey(IntrinsicTagNames.resourceId).getValue())
                .isEqualTo("node[1].nodeSnmp[]");
        assertThat(out.getFirstTagByKey("node").getValue()).isEqualTo("1:1");
        assertThat(out.getFirstTagByKey("if_name").getValue()).isEqualTo("eth0");
        assertThat(out.getFirstTagByKey(MetaTagNames.mtype).getValue()).isEqualTo("gauge");
    }

    @Test
    void passes_through_when_mtype_already_present() {
        PluginMetrics metrics = new PluginMetrics();
        MtypeFallback fallback = new MtypeFallback(metrics);

        Metric input = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "ifHCInOctets")
                .metaTag(MetaTagNames.mtype, "counter")
                .build();

        Metric out = fallback.apply(input);

        assertThat(out).isSameAs(input);
        assertThat(out.getFirstTagByKey(MetaTagNames.mtype).getValue()).isEqualTo("counter");
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_SYNTHESIZED_MTYPE).longValue())
                .isZero();
    }

    @Test
    void treats_empty_mtype_value_as_missing() {
        // An empty-valued mtype label round-tripped through Prometheus could
        // happen if a misbehaving operator wrote an explicit empty label —
        // Prometheus treats empty-value labels as absent at query time, but
        // the JSON parser would still hand us an empty-string tag. Treat as
        // missing so OpenNMS-core's Mtype.valueOf() doesn't see "".
        PluginMetrics metrics = new PluginMetrics();
        MtypeFallback fallback = new MtypeFallback(metrics);
        Metric input = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "x")
                .metaTag(MetaTagNames.mtype, "")
                .build();

        Metric out = fallback.apply(input);

        assertThat(out.getFirstTagByKey(MetaTagNames.mtype).getValue()).isEqualTo("gauge");
        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_SYNTHESIZED_MTYPE).longValue())
                .isEqualTo(1);
    }

    @Test
    void counter_increments_per_synthesis() {
        PluginMetrics metrics = new PluginMetrics();
        MtypeFallback fallback = new MtypeFallback(metrics);

        for (int i = 0; i < 5; i++) {
            fallback.apply(ImmutableMetric.builder()
                    .intrinsicTag(IntrinsicTagNames.name, "metric-" + i)
                    .build());
        }

        assertThat(metrics.snapshot().get(PluginMetrics.SAMPLES_SYNTHESIZED_MTYPE).longValue())
                .isEqualTo(5);
    }

    @Test
    void warns_once_per_metric_name() {
        MtypeFallback fallback = new MtypeFallback(new PluginMetrics());

        // Three syntheses for the same metric name → one entry in the WARN
        // tracking set (i.e. one log line was emitted).
        for (int i = 0; i < 3; i++) {
            fallback.apply(ImmutableMetric.builder()
                    .intrinsicTag(IntrinsicTagNames.name, "ifHCInOctets")
                    .build());
        }

        assertThat(fallback.warnedMetricsForTesting())
                .containsExactly("ifHCInOctets");
    }

    @Test
    void warn_set_is_bounded_at_cap_via_lru_eviction() {
        MtypeFallback fallback = new MtypeFallback(new PluginMetrics());

        // Feed CAP+1 distinct metric names. The eldest insertion (metric-0)
        // should be evicted; the set holds CAP entries.
        for (int i = 0; i <= MtypeFallback.WARN_TRACKING_CAP; i++) {
            fallback.apply(ImmutableMetric.builder()
                    .intrinsicTag(IntrinsicTagNames.name, "metric-" + i)
                    .build());
        }

        var warned = fallback.warnedMetricsForTesting();
        assertThat(warned).hasSize(MtypeFallback.WARN_TRACKING_CAP);
        // The very first one inserted has been evicted to make room.
        assertThat(warned).doesNotContain("metric-0");
        // The most recent ones survive.
        assertThat(warned).contains("metric-" + MtypeFallback.WARN_TRACKING_CAP);
    }

    @Test
    void null_metrics_sink_is_tolerated() {
        // Constructor accepts null PluginMetrics for tests that don't care.
        MtypeFallback fallback = new MtypeFallback(null);
        Metric input = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "x")
                .build();

        Metric out = fallback.apply(input);

        assertThat(out.getFirstTagByKey(MetaTagNames.mtype).getValue()).isEqualTo("gauge");
    }

}
