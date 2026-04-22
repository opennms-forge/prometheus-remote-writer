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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

// Force sequential execution: several tests here share static state
// (INSTANCE_ID_UNSET_WARNED, LAST_ACTIVE) that is intentionally JVM-scoped.
// Parallel execution within this class would race the @BeforeEach reset
// against a concurrent test's start(). Sequential today, immune to a
// future project-wide parallel-tests switch.
@Execution(ExecutionMode.SAME_THREAD)
class PrometheusRemoteWriterStorageTest {

    @BeforeEach
    void resetInstanceIdWarnGate() {
        // The WARN gate is a static one-shot; tests that start more than one
        // storage bean need a clean slate so the gate can be observed flipping.
        PrometheusRemoteWriterStorage.resetInstanceIdWarnedForTesting();
    }

    // ---------- instance.id startup WARN ------------------------------------

    @Test
    void instance_id_unset_trips_the_warn_gate_on_start() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isFalse();
        s.start();
        try {
            assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isTrue();
        } finally {
            s.stop();
        }
    }

    @Test
    void instance_id_set_does_not_trip_the_warn_gate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("opennms-us-east");
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        s.start();
        try {
            assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isFalse();
        } finally {
            s.stop();
        }
    }

    @Test
    void warn_gate_stays_tripped_across_hot_reload_activations() {
        // Simulates the blueprint rebuild-on-config-update path. The gate is
        // static so the WARN fires exactly once per JVM regardless of how many
        // times the blueprint container re-activates the bean with unset
        // instance.id.
        PrometheusRemoteWriterStorage first = new PrometheusRemoteWriterStorage(minimal());
        first.start();
        first.stop();
        assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isTrue();

        PrometheusRemoteWriterStorage second = new PrometheusRemoteWriterStorage(minimal());
        second.start();
        try {
            // CAS already happened; a second start() cannot re-flip the gate.
            assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isTrue();
        } finally {
            second.stop();
        }
    }

    @Test
    void warn_gate_not_tripped_when_config_is_invalid() {
        // If validate() fails, warnIfInstanceIdUnset never runs — we don't
        // want spurious WARNs while ConfigAdmin is still settling.
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        // deliberately leave write.url / read.url unset so validate() throws
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        s.start();
        assertThat(PrometheusRemoteWriterStorage.isInstanceIdWarnedForTesting()).isFalse();
    }

    @Test
    void reserved_rename_target_leaves_service_inactive() {
        // A labels.rename whose target collides with a default-allowlist name
        // is rejected by validate(); start() catches the IllegalStateException
        // and leaves `active` null so OpenNMS sees no writable TSS until the
        // operator corrects the cfg.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foreign_source -> __name__");
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
        assertThatCode(s::start).doesNotThrowAnyException();
        assertThatThrownBy(() -> s.store(List.of()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("not accepting writes");
    }

    @Test
    void start_tolerates_invalid_config_and_stays_inactive() {
        // start() must not throw even when config is bad — throwing would
        // permanently kill the blueprint container and defeat the
        // reload-on-config-update strategy (see PrometheusRemoteWriterStorage#start).
        // Instead the bundle registers the service but refuses writes until
        // a later reload delivers valid config.
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("u");
        c.setBasicPassword("p");
        c.setBearerToken("t");

        PrometheusRemoteWriterStorage storage = new PrometheusRemoteWriterStorage(c);
        assertThatCode(storage::start).doesNotThrowAnyException();
        assertThatThrownBy(() -> storage.store(List.of()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("not accepting writes");
    }

    @Test
    void start_and_stop_with_minimum_valid_config() {
        PrometheusRemoteWriterStorage storage = new PrometheusRemoteWriterStorage(minimal());
        assertThatCode(storage::start).doesNotThrowAnyException();
        assertThatCode(storage::stop).doesNotThrowAnyException();
    }

    @Test
    void supports_aggregation_is_true_only_for_none() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        assertThat(s.supportsAggregation(Aggregation.NONE)).isTrue();
        assertThat(s.supportsAggregation(Aggregation.AVERAGE)).isFalse();
        assertThat(s.supportsAggregation(Aggregation.MIN)).isFalse();
        assertThat(s.supportsAggregation(Aggregation.MAX)).isFalse();
    }

    @Test
    void delete_is_a_noop_that_counts_and_never_throws() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        var metric = ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                .build();

        assertThatCode(() -> s.delete(metric)).doesNotThrowAnyException();
        assertThatCode(() -> s.delete(metric)).doesNotThrowAnyException();
        assertThatCode(() -> s.delete(metric)).doesNotThrowAnyException();

        assertThat(s.getDeleteNoopTotal()).isEqualTo(3);
    }

    @Test
    void hot_reload_emits_diff_without_throwing() {
        // Activate once.
        PrometheusRemoteWriterStorage first = new PrometheusRemoteWriterStorage(minimal());
        first.start();

        // Activate a second time with a modified config — simulates Blueprint
        // rebuilding the bean after a ConfigAdmin update. LAST_ACTIVE is
        // shared state inside the storage, so the second start() should see
        // the diff and log it.
        PrometheusRemoteWriterConfig c2 = minimal();
        c2.setBatchSize(500);
        PrometheusRemoteWriterStorage second = new PrometheusRemoteWriterStorage(c2);
        assertThatCode(second::start).doesNotThrowAnyException();
    }

    private static PrometheusRemoteWriterConfig minimal() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        return c;
    }

    // ---------- end-to-end store() flow ------------------------------------

    @Test
    void store_maps_enqueues_and_flushes_end_to_end() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse().setResponseCode(204));

            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setBatchSize(10);
            c.setFlushIntervalMs(50);
            c.setShutdownGracePeriodMs(1_000);

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
            s.start();
            try {
                s.store(List.of(
                        ImmutableSample.builder()
                                .metric(ImmutableMetric.builder()
                                        .intrinsicTag("name", "ifHCInOctets")
                                        .intrinsicTag("resourceId", "node[1].interfaceSnmp[eth0]")
                                        .externalTag("nodeId", "1")
                                        .build())
                                .time(Instant.ofEpochMilli(1_000_000L))
                                .value(42.0)
                                .build()));

                PluginMetrics m = s.getMetrics();
                await().atMost(Duration.ofSeconds(2))
                       .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_WRITTEN).longValue() == 1L);
            } finally {
                s.stop();
            }
            assertThat(server.getRequestCount()).isEqualTo(1);
        }
    }

    @Test
    void store_throws_when_not_started() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        assertThatThrownBy(() -> s.store(List.of()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("not accepting writes");
    }

    @Test
    void second_start_on_same_bean_is_a_no_op() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        assertThatCode(s::start).doesNotThrowAnyException();
        try {
            PluginMetrics first = s.getMetrics();
            assertThatCode(s::start).doesNotThrowAnyException();
            PluginMetrics second = s.getMetrics();
            // Idempotent: the existing pipeline is preserved, not rebuilt.
            assertThat(second).isSameAs(first);
        } finally {
            s.stop();
        }
    }

    @Test
    void stop_then_start_produces_a_fresh_pipeline() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        s.start();
        PluginMetrics first = s.getMetrics();
        s.stop();
        assertThat(s.getMetrics()).isNull();

        s.start();
        try {
            PluginMetrics second = s.getMetrics();
            assertThat(second).isNotNull();
            assertThat(second).isNotSameAs(first);
        } finally {
            s.stop();
        }
    }

    @Test
    void http_in_flight_gauge_is_registered() {
        PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(minimal());
        s.start();
        try {
            assertThat(s.getMetrics().snapshot()).containsKey(PluginMetrics.HTTP_IN_FLIGHT);
        } finally {
            s.stop();
        }
    }

    @Test
    void backend_returning_4xx_increments_drop_counter() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse().setResponseCode(400).setBody("bad labels"));

            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setBatchSize(10);
            c.setFlushIntervalMs(50);
            c.setShutdownGracePeriodMs(1_000);

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
            s.start();
            try {
                s.store(List.of(ImmutableSample.builder()
                        .metric(ImmutableMetric.builder()
                                .intrinsicTag("name", "t")
                                .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                                .build())
                        .time(Instant.ofEpochMilli(1_000_000L))
                        .value(1.0)
                        .build()));

                PluginMetrics m = s.getMetrics();
                await().atMost(Duration.ofSeconds(2))
                       .until(() -> m.snapshot().get(PluginMetrics.SAMPLES_DROPPED_4XX).longValue() == 1L);
            } finally {
                s.stop();
            }
        }
    }

    @Test
    void queue_overflow_throws_storage_exception_with_counter_increment() throws Exception {
        // Stall the flusher inside its first HTTP write so queued samples
        // accumulate until the queue is full. NO_RESPONSE keeps the socket
        // open indefinitely; OkHttp's read timeout is deliberately long so it
        // doesn't rescue us during the test window.
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new okhttp3.mockwebserver.MockResponse()
                    .setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE));

            PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
            c.setWriteUrl(server.url("/api/v1/push").toString());
            c.setReadUrl(server.url("/prometheus").toString());
            c.setQueueCapacity(2);
            c.setBatchSize(1);
            c.setFlushIntervalMs(50);
            c.setHttpReadTimeoutMs(60_000);
            c.setHttpWriteTimeoutMs(60_000);
            c.setRetryInitialBackoffMs(1);
            c.setRetryMaxBackoffMs(2);
            c.setRetryMaxAttempts(1);
            c.setShutdownGracePeriodMs(100);

            PrometheusRemoteWriterStorage s = new PrometheusRemoteWriterStorage(c);
            s.start();
            try {
                // First store: flusher picks it up immediately, hits NO_RESPONSE,
                // hangs. Wait for the flusher to be parked in the HTTP call.
                s.store(List.of(sample("a")));
                server.takeRequest(5, TimeUnit.SECONDS);

                // Now fill the queue to capacity.
                s.store(List.of(sample("b")));
                s.store(List.of(sample("c")));

                assertThatThrownBy(() -> s.store(List.of(sample("d"))))
                        .isInstanceOf(StorageException.class)
                        .hasMessageContaining("queue full");

                PluginMetrics m = s.getMetrics();
                assertThat(m.snapshot().get(PluginMetrics.SAMPLES_DROPPED_QUEUE_FULL).longValue())
                        .isEqualTo(1L);
            } finally {
                s.stop();
            }
        }
    }

    private static org.opennms.integration.api.v1.timeseries.Sample sample(String id) {
        return ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", "t")
                        .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                        .externalTag("id", id)
                        .build())
                .time(Instant.now())
                .value(1.0)
                .build();
    }
}
