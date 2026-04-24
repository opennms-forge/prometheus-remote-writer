/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.PrometheusRemoteWriterStorage;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

class StatsCommandTest {

    @Test
    void renders_message_when_storage_is_not_active() {
        StatsCommand cmd = new StatsCommand((PrometheusRemoteWriterStorage) null);
        assertThat(capture(cmd)).contains("not active");
    }

    @Test
    void renders_message_when_storage_has_not_been_started() {
        PrometheusRemoteWriterStorage storage = mock(PrometheusRemoteWriterStorage.class);
        when(storage.getMetrics()).thenReturn(null);
        StatsCommand cmd = new StatsCommand(storage);
        assertThat(capture(cmd)).contains("not been started");
    }

    @Test
    void renders_all_counters_and_gauges_from_snapshot() {
        PluginMetrics metrics = new PluginMetrics();
        metrics.samplesWritten(42);
        metrics.samplesDropped4xx(3);
        metrics.samplesUnparseableResourceId(5);
        metrics.registerLongGauge(PluginMetrics.QUEUE_DEPTH, () -> 7L);

        PrometheusRemoteWriterStorage storage = mock(PrometheusRemoteWriterStorage.class);
        when(storage.getMetrics()).thenReturn(metrics);

        String out = capture(new StatsCommand(storage));

        assertThat(out).contains("samples_written_total");
        assertThat(out).contains("42");
        assertThat(out).contains("samples_dropped_4xx_total");
        assertThat(out).contains("3");
        assertThat(out).contains("samples_unparseable_resource_id_total");
        assertThat(out).contains("5");
        assertThat(out).contains("queue_depth");
        assertThat(out).contains("7");
        // Header
        assertThat(out).contains("prometheus-remote-writer metrics");
    }

    private static String capture(StatsCommand cmd) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        cmd.render(new PrintStream(baos, true, StandardCharsets.UTF_8));
        return baos.toString(StandardCharsets.UTF_8);
    }
}
