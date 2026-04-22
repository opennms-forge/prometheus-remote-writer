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
        // on the read side — partition-lossy is by design).
        var keys = m0.getMetaTags().stream().map(t -> t.getKey()).toList();
        assertThat(keys).contains(
                "node", "foreign_source", "foreign_id", "location",
                "resource_type", "resource_instance", "if_name", "if_speed");
        assertThat(keys).contains("onms_cat_Routers", "onms_cat_ProductionSites");
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
        // OpenNMS instances writing into the same backend.
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
        storage = new PrometheusRemoteWriterStorage(c);
        storage.start();

        Instant now = Instant.now();
        String metricName = "onms_it_id_" + System.nanoTime();
        storage.store(List.of(ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", metricName)
                        .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                        .build())
                .time(now)
                .value(1.0)
                .build()));

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

        // The onms_instance_id label is present on the round-tripped series
        // with the value we configured.
        var tags = found.get(0).getMetaTags();
        assertThat(tags).anySatisfy(t -> {
            assertThat(t.getKey()).isEqualTo("onms_instance_id");
            assertThat(t.getValue()).isEqualTo("it-test");
        });
    }
}
