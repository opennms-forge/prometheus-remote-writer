/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration test against a real Prometheus instance running in a
 * Testcontainers-managed container. Exercises the full pipeline: store() →
 * label mapping → queue → flusher → snappy serialization → Remote Write v1
 * HTTP POST → Prometheus ingestion → Prometheus HTTP query API → read path.
 *
 * <p>Requires Docker; runs in the {@code verify} phase (named {@code *IT.java}).
 */
@Testcontainers
class PrometheusRemoteWriteIT {

    @Container
    static GenericContainer<?> prometheus =
            new GenericContainer<>(DockerImageName.parse("prom/prometheus:v2.53.2"))
                    .withExposedPorts(9090)
                    .withCommand(
                            "--config.file=/etc/prometheus/prometheus.yml",
                            "--storage.tsdb.path=/prometheus",
                            "--web.console.libraries=/usr/share/prometheus/console_libraries",
                            "--web.console.templates=/usr/share/prometheus/consoles",
                            "--web.enable-remote-write-receiver")
                    .waitingFor(Wait.forHttp("/-/ready").forStatusCode(200));

    private PrometheusRemoteWriterStorage storage;

    @BeforeEach
    void start() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setReadUrl(base);
        c.setBatchSize(10);
        c.setFlushIntervalMs(100);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
        c.setRetryMaxAttempts(10);
        c.setShutdownGracePeriodMs(2_000);

        storage = new PrometheusRemoteWriterStorage(c);
        storage.start();
    }

    @AfterEach
    void stop() {
        if (storage != null) storage.stop();
    }

    @Test
    void write_sample_then_query_it_back_through_prometheus() throws Exception {
        Instant now = Instant.now();

        // Use a metric name that's unique per run so any stale test state doesn't interfere.
        String metricName = "onms_it_" + System.nanoTime();
        Sample sample = ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", metricName)
                        .intrinsicTag("resourceId", "nodeSource[NOC:it-host].interfaceSnmp[eth0]")
                        .externalTag("foreignSource", "NOC")
                        .externalTag("foreignId", "it-host")
                        .externalTag("location", "lab")
                        .externalTag("ifName", "eth0")
                        .externalTag("ifHighSpeed", "1000")
                        .externalTag("categories", "Routers, ProductionSites")
                        .build())
                .time(now)
                .value(42.0)
                .build();

        storage.store(List.of(sample));

        // Wait for Prometheus to ingest (asynchronous write → flush → scrape window).
        PluginMetrics m = storage.getMetrics();
        await().atMost(Duration.ofSeconds(20))
               .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

        // findMetrics through the plugin's read path.
        TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key("name")
                .value(metricName)
                .build();

        List<Metric> found = await().atMost(Duration.ofSeconds(20))
                .until(() -> storage.findMetrics(List.of(nameMatcher)),
                       list -> !list.isEmpty());
        assertThat(found).hasSize(1);
        Metric m0 = found.get(0);

        // The default-allowlist labels should all round-trip (as meta tags
        // on the read side — partition-lossy is by design). `job` and
        // `instance` joined the default set in v0.4.
        var keys = m0.getMetaTags().stream().map(t -> t.getKey()).toList();
        assertThat(keys).contains(
                "job", "instance",
                "node", "foreign_source", "foreign_id", "location",
                "resource_type", "resource_instance", "if_name", "if_speed");
        assertThat(keys).contains("onms_cat_Routers", "onms_cat_ProductionSites");

        // job derives to "snmp" from a bracketed-form resourceId; instance
        // mirrors node for the FS-qualified identity.
        assertThat(m0.getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("job");
                    assertThat(t.getValue()).isEqualTo("snmp");
                })
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("instance");
                    assertThat(t.getValue()).isEqualTo("NOC:it-host");
                });
        // Negative assertion: default fixture does not set instance.id, so the
        // origin-identity label MUST NOT appear.
        assertThat(keys).doesNotContain("onms_instance_id");

        // getTimeSeriesData returns the data point we wrote.
        TimeSeriesData data = storage.getTimeSeriesData(
                ImmutableTimeSeriesFetchRequest.builder()
                        .metric(m0)
                        .start(now.minusSeconds(60))
                        .end(now.plusSeconds(60))
                        .step(Duration.ofSeconds(1))
                        .aggregation(org.opennms.integration.api.v1.timeseries.Aggregation.NONE)
                        .build());
        List<DataPoint> points = data.getDataPoints();
        assertThat(points).isNotEmpty();
        assertThat(points.get(0).getValue()).isEqualTo(42.0);
    }

    @Test
    void instance_id_is_emitted_and_round_trips_through_prometheus() throws Exception {
        // Rebuild the storage with instance.id set — simulates one of multiple
        // OpenNMS instances writing into the same backend. Uses a local
        // override + try/finally so this test's storage is stopped
        // deterministically, independent of the @AfterEach which targets the
        // fixture's default-config storage.
        storage.stop();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setReadUrl(base);
        c.setBatchSize(10);
        c.setFlushIntervalMs(100);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
        c.setRetryMaxAttempts(10);
        c.setShutdownGracePeriodMs(2_000);
        c.setInstanceId("it-test");
        PrometheusRemoteWriterStorage override = new PrometheusRemoteWriterStorage(c);
        override.start();
        try {
            Instant now = Instant.now();
            String metricName = "onms_it_id_" + System.nanoTime();
            override.store(List.of(ImmutableSample.builder()
                    .metric(ImmutableMetric.builder()
                            .intrinsicTag("name", metricName)
                            .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                            .build())
                    .time(now)
                    .value(1.0)
                    .build()));

            PluginMetrics m = override.getMetrics();
            await().atMost(Duration.ofSeconds(20))
                   .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

            TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                    .type(TagMatcher.Type.EQUALS)
                    .key("name")
                    .value(metricName)
                    .build();

            List<Metric> found = await().atMost(Duration.ofSeconds(20))
                    .until(() -> override.findMetrics(List.of(nameMatcher)),
                           list -> !list.isEmpty());
            assertThat(found).hasSize(1);

            // The onms_instance_id label is present on the round-tripped
            // series with the value we configured.
            var tags = found.get(0).getMetaTags();
            assertThat(tags).anySatisfy(t -> {
                assertThat(t.getKey()).isEqualTo("onms_instance_id");
                assertThat(t.getValue()).isEqualTo("it-test");
            });
        } finally {
            override.stop();
        }
    }

    @Test
    void slash_fs_resource_id_acquires_node_and_parsed_components() throws Exception {
        // Self-monitor / JMX-shaped resourceId: `snmp/fs/<fs>/<fid>/<group>/<instance>`.
        // Previously these samples reached Prometheus with only `{resourceId="..."}`;
        // v0.2 parses the slash-path and emits `node`, `resource_type`,
        // `resource_instance` from the resourceId alone.
        Instant now = Instant.now();
        String metricName = "onms_it_slashfs_" + System.nanoTime();
        Sample sample = ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", metricName)
                        .intrinsicTag("resourceId",
                                "snmp/fs/selfmonitor/1/opennms-jvm/OpenNMS_Name_Notifd")
                        .build())
                .time(now)
                .value(7.5)
                .build();

        storage.store(List.of(sample));

        PluginMetrics m = storage.getMetrics();
        await().atMost(Duration.ofSeconds(20))
               .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

        TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key("name")
                .value(metricName)
                .build();

        List<Metric> found = await().atMost(Duration.ofSeconds(20))
                .until(() -> storage.findMetrics(List.of(nameMatcher)),
                       list -> !list.isEmpty());
        assertThat(found).hasSize(1);

        var keys = found.get(0).getMetaTags().stream().map(t -> t.getKey()).toList();
        assertThat(keys).contains("node", "resource_type", "resource_instance");

        // Values round-trip: parsed from the slash-FS grammar, `<fs>:<fid>` for node.
        assertThat(found.get(0).getMetaTags())
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("node");
                    assertThat(t.getValue()).isEqualTo("selfmonitor:1");
                })
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("resource_type");
                    assertThat(t.getValue()).isEqualTo("opennms-jvm");
                })
                .anySatisfy(t -> {
                    assertThat(t.getKey()).isEqualTo("resource_instance");
                    assertThat(t.getValue()).isEqualTo("OpenNMS_Name_Notifd");
                });
    }

    @Test
    void labels_include_star_does_not_introduce_duplicate_meta_tags() throws Exception {
        // Rebuild the storage with labels.include = * — the canonical footgun
        // that v0.1 turned into duplicate labels (`name` alongside `__name__`,
        // `resource_id` alongside `resourceId`). v0.2's consumed-keys dedup
        // means these source keys are skipped; the read-back series must NOT
        // carry meta tags with those names.
        storage.stop();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setReadUrl(base);
        c.setLabelsInclude("*");
        c.setBatchSize(10);
        c.setFlushIntervalMs(100);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
        c.setRetryMaxAttempts(10);
        c.setShutdownGracePeriodMs(2_000);
        PrometheusRemoteWriterStorage override = new PrometheusRemoteWriterStorage(c);
        override.start();
        try {
            Instant now = Instant.now();
            String metricName = "onms_it_dedup_" + System.nanoTime();
            Sample sample = ImmutableSample.builder()
                    .metric(ImmutableMetric.builder()
                            .intrinsicTag("name", metricName)
                            .intrinsicTag("resourceId", "nodeSource[NOC:it-dedup].interfaceSnmp[eth0]")
                            .externalTag("foreignSource", "NOC")
                            .externalTag("foreignId", "it-dedup")
                            .externalTag("nodeLabel", "it-dedup.example.com")
                            .externalTag("location", "lab")
                            .externalTag("ifName", "eth0")
                            .externalTag("ifDescr", "GigabitEthernet0/0")
                            .externalTag("ifHighSpeed", "1000")
                            .externalTag("ifSpeed", "4294967295")
                            .externalTag("nodeId", "42")
                            .externalTag("categories", "Routers")
                            .build())
                    .time(now)
                    .value(3.0)
                    .build();

            override.store(List.of(sample));

            PluginMetrics m = override.getMetrics();
            await().atMost(Duration.ofSeconds(20))
                   .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

            TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                    .type(TagMatcher.Type.EQUALS)
                    .key("name")
                    .value(metricName)
                    .build();

            List<Metric> found = await().atMost(Duration.ofSeconds(20))
                    .until(() -> override.findMetrics(List.of(nameMatcher)),
                           list -> !list.isEmpty());
            assertThat(found).hasSize(1);

            // The five v0.1 duplicates must NOT appear as meta tags.
            var keys = found.get(0).getMetaTags().stream().map(t -> t.getKey()).toList();
            assertThat(keys).doesNotContain("name", "resource_id", "if_high_speed", "node_id", "categories");

            // The canonical defaults ARE still present.
            assertThat(keys).contains(
                    "node", "foreign_source", "foreign_id", "node_label", "location",
                    "if_name", "if_descr", "if_speed", "onms_cat_Routers");
        } finally {
            override.stop();
        }
    }

    @Test
    void labels_copy_emits_second_label_that_round_trips() throws Exception {
        // labels.copy = node -> cluster: the same value is emitted under
        // two label names. Prometheus stores both; findMetrics surfaces both
        // as meta tags with equal values. Uses `cluster` rather than
        // `instance` because v0.4 reserved `instance` (now a default emit).
        storage.stop();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setReadUrl(base);
        c.setLabelsCopy("node -> cluster");
        c.setBatchSize(10);
        c.setFlushIntervalMs(100);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
        c.setRetryMaxAttempts(10);
        c.setShutdownGracePeriodMs(2_000);
        PrometheusRemoteWriterStorage override = new PrometheusRemoteWriterStorage(c);
        override.start();
        try {
            Instant now = Instant.now();
            String metricName = "onms_it_copy_" + System.nanoTime();
            Sample sample = ImmutableSample.builder()
                    .metric(ImmutableMetric.builder()
                            .intrinsicTag("name", metricName)
                            .intrinsicTag("resourceId", "nodeSource[NOC:it-copy].interfaceSnmp[eth0]")
                            .externalTag("foreignSource", "NOC")
                            .externalTag("foreignId", "it-copy")
                            .build())
                    .time(now)
                    .value(1.0)
                    .build();

            override.store(List.of(sample));

            PluginMetrics m = override.getMetrics();
            await().atMost(Duration.ofSeconds(20))
                   .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

            TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                    .type(TagMatcher.Type.EQUALS)
                    .key("name")
                    .value(metricName)
                    .build();

            List<Metric> found = await().atMost(Duration.ofSeconds(20))
                    .until(() -> override.findMetrics(List.of(nameMatcher)),
                           list -> !list.isEmpty());
            assertThat(found).hasSize(1);

            var tags = found.get(0).getMetaTags();
            assertThat(tags).anySatisfy(t -> {
                assertThat(t.getKey()).isEqualTo("node");
                assertThat(t.getValue()).isEqualTo("NOC:it-copy");
            });
            assertThat(tags).anySatisfy(t -> {
                assertThat(t.getKey()).isEqualTo("cluster");
                assertThat(t.getValue()).isEqualTo("NOC:it-copy");
            });
        } finally {
            override.stop();
        }
    }

    @Test
    void labels_copy_combined_with_rename_emits_both_renamed_and_copied_labels() throws Exception {
        // labels.copy = node -> cluster AND labels.rename = node -> host:
        // copy runs first on pre-rename `node`, rename then moves `node` to
        // `host`. Both `host` and `cluster` reach Prometheus with the same
        // node value; `node` itself is gone. Uses `cluster` rather than
        // `instance` because v0.4 reserved `instance` (now a default emit).
        storage.stop();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setReadUrl(base);
        c.setLabelsCopy("node -> cluster");
        c.setLabelsRename("node -> host");
        c.setBatchSize(10);
        c.setFlushIntervalMs(100);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
        c.setRetryMaxAttempts(10);
        c.setShutdownGracePeriodMs(2_000);
        PrometheusRemoteWriterStorage override = new PrometheusRemoteWriterStorage(c);
        override.start();
        try {
            Instant now = Instant.now();
            String metricName = "onms_it_copyrename_" + System.nanoTime();
            Sample sample = ImmutableSample.builder()
                    .metric(ImmutableMetric.builder()
                            .intrinsicTag("name", metricName)
                            .intrinsicTag("resourceId", "nodeSource[NOC:it-copyrename].interfaceSnmp[eth0]")
                            .externalTag("foreignSource", "NOC")
                            .externalTag("foreignId", "it-copyrename")
                            .build())
                    .time(now)
                    .value(2.0)
                    .build();

            override.store(List.of(sample));

            PluginMetrics m = override.getMetrics();
            await().atMost(Duration.ofSeconds(20))
                   .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

            TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                    .type(TagMatcher.Type.EQUALS)
                    .key("name")
                    .value(metricName)
                    .build();

            List<Metric> found = await().atMost(Duration.ofSeconds(20))
                    .until(() -> override.findMetrics(List.of(nameMatcher)),
                           list -> !list.isEmpty());
            assertThat(found).hasSize(1);

            var keys = found.get(0).getMetaTags().stream().map(t -> t.getKey()).toList();
            assertThat(keys).contains("host", "cluster");
            assertThat(keys).doesNotContain("node");

            assertThat(found.get(0).getMetaTags())
                    .anySatisfy(t -> {
                        assertThat(t.getKey()).isEqualTo("host");
                        assertThat(t.getValue()).isEqualTo("NOC:it-copyrename");
                    })
                    .anySatisfy(t -> {
                        assertThat(t.getKey()).isEqualTo("cluster");
                        assertThat(t.getValue()).isEqualTo("NOC:it-copyrename");
                    });
        } finally {
            override.stop();
        }
    }

    @Test
    void job_derives_to_jmx_for_slash_fs_resource_id() throws Exception {
        // Self-monitor / JMX-shaped resourceId: `snmp/fs/<fs>/<fid>/jmx-*/<instance>`.
        // The plugin's job derivation recognises the `jmx-*` (and
        // `opennms-jvm`) group segments and emits `job="jmx"`, distinct from
        // the `job="snmp"` default for regular collection. This lets
        // operators scope dashboards to JVM-collected metrics with
        // `{job="jmx"}`.
        Instant now = Instant.now();
        String metricName = "onms_it_jmx_" + System.nanoTime();
        Sample sample = ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", metricName)
                        .intrinsicTag("resourceId",
                                "snmp/fs/selfmonitor/1/jmx-minion/OpenNMS_Name_Notifd")
                        .build())
                .time(now)
                .value(3.14)
                .build();

        storage.store(List.of(sample));

        PluginMetrics m = storage.getMetrics();
        await().atMost(Duration.ofSeconds(20))
               .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

        TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key("name")
                .value(metricName)
                .build();

        List<Metric> found = await().atMost(Duration.ofSeconds(20))
                .until(() -> storage.findMetrics(List.of(nameMatcher)),
                       list -> !list.isEmpty());
        assertThat(found).hasSize(1);

        var tags = found.get(0).getMetaTags();
        assertThat(tags).anySatisfy(t -> {
            assertThat(t.getKey()).isEqualTo("job");
            assertThat(t.getValue()).isEqualTo("jmx");
        });
        // Also confirm instance is populated from the parsed slash-FS identity.
        assertThat(tags).anySatisfy(t -> {
            assertThat(t.getKey()).isEqualTo("instance");
            assertThat(t.getValue()).isEqualTo("selfmonitor:1");
        });
    }

    @Test
    void job_name_override_replaces_derivation() throws Exception {
        // labels.copy is one config; this is a different knob: job.name
        // forces every sample to carry `job=<this value>` regardless of
        // resourceId shape. Useful when an operator wants fleet-wide
        // consistency (e.g., `opennms-prod` across SNMP and JMX data).
        storage.stop();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        String base = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        c.setWriteUrl(base + "/api/v1/write");
        c.setReadUrl(base);
        c.setJobName("opennms-prod");
        c.setBatchSize(10);
        c.setFlushIntervalMs(100);
        c.setRetryInitialBackoffMs(100);
        c.setRetryMaxBackoffMs(500);
        c.setRetryMaxAttempts(10);
        c.setShutdownGracePeriodMs(2_000);
        PrometheusRemoteWriterStorage override = new PrometheusRemoteWriterStorage(c);
        override.start();
        try {
            Instant now = Instant.now();
            String metricName = "onms_it_jobname_" + System.nanoTime();
            // Bracketed resourceId — would otherwise derive to job="snmp".
            Sample sample = ImmutableSample.builder()
                    .metric(ImmutableMetric.builder()
                            .intrinsicTag("name", metricName)
                            .intrinsicTag("resourceId", "nodeSource[NOC:it-jobname].interfaceSnmp[eth0]")
                            .externalTag("foreignSource", "NOC")
                            .externalTag("foreignId", "it-jobname")
                            .build())
                    .time(now)
                    .value(1.0)
                    .build();

            override.store(List.of(sample));

            PluginMetrics m = override.getMetrics();
            await().atMost(Duration.ofSeconds(20))
                   .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() >= 1L);

            TagMatcher nameMatcher = ImmutableTagMatcher.builder()
                    .type(TagMatcher.Type.EQUALS)
                    .key("name")
                    .value(metricName)
                    .build();

            List<Metric> found = await().atMost(Duration.ofSeconds(20))
                    .until(() -> override.findMetrics(List.of(nameMatcher)),
                           list -> !list.isEmpty());
            assertThat(found).hasSize(1);

            assertThat(found.get(0).getMetaTags()).anySatisfy(t -> {
                assertThat(t.getKey()).isEqualTo("job");
                assertThat(t.getValue()).isEqualTo("opennms-prod");
            });
        } finally {
            override.stop();
        }
    }
}
