/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;

class PrometheusReadClientTest {

    private MockWebServer server;
    private PrometheusReadClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("").toString().replaceAll("/$", ""));
        c.validate();
        client = new PrometheusReadClient(c);
    }

    @AfterEach
    void tearDown() throws IOException {
        client.shutdown();
        server.shutdown();
    }

    @Test
    void find_metrics_hits_series_endpoint_with_expected_query() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("{\"status\":\"success\",\"data\":["
                       + "{\"__name__\":\"ifHCInOctets\",\"node\":\"1:1\"}"
                       + "]}"));

        TagMatcher m = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key("name")
                .value("ifHCInOctets")
                .build();
        List<Metric> out = client.findMetrics(List.of(m));

        assertThat(out).hasSize(1);
        RecordedRequest req = server.takeRequest();
        // OkHttp leaves [ and ] as literals in the path since they're in
        // RFC 3986's gen-delims; inner special chars (= and ") are escaped.
        assertThat(req.getPath()).startsWith("/api/v1/series?match[]=");
        assertThat(req.getPath()).contains("__name__%3D%22ifHCInOctets%22");
        assertThat(req.getPath()).contains("&start=");
    }

    @Test
    void find_metrics_rejects_null_or_empty_matchers() {
        assertThatThrownBy(() -> client.findMetrics(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> client.findMetrics(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void get_time_series_data_hits_query_range_with_derived_step() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":["
                       + "{\"metric\":{},\"values\":[[1700000000,\"1.0\"],[1700000060,\"2.0\"]]}"
                       + "]}}"));

        TimeSeriesFetchRequest request = new FakeFetchRequest(
                ImmutableMetric.builder().intrinsicTag("name", "foo").build(),
                Instant.ofEpochSecond(1_700_000_000L),
                Instant.ofEpochSecond(1_700_000_120L),
                Duration.ofSeconds(60));
        TimeSeriesData data = client.getTimeSeriesData(request);

        List<DataPoint> pts = data.getDataPoints();
        assertThat(pts).hasSize(2);
        assertThat(pts.get(1).getValue()).isEqualTo(2.0);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).startsWith("/api/v1/query_range?");
        assertThat(req.getPath()).contains("step=60s");
    }

    @Test
    void step_derivation_respects_explicit_step() {
        TimeSeriesFetchRequest request = new FakeFetchRequest(
                ImmutableMetric.builder().intrinsicTag("name", "foo").build(),
                Instant.ofEpochSecond(0),
                Instant.ofEpochSecond(3600),
                Duration.ofSeconds(15));
        assertThat(PrometheusReadClient.stepSeconds(request)).isEqualTo(15);
    }

    @Test
    void step_derivation_falls_back_to_range_over_600_when_no_step_provided() {
        TimeSeriesFetchRequest request = new FakeFetchRequest(
                ImmutableMetric.builder().intrinsicTag("name", "foo").build(),
                Instant.ofEpochSecond(0),
                Instant.ofEpochSecond(6000),
                null);
        // 6000s / 600 points = 10s step
        assertThat(PrometheusReadClient.stepSeconds(request)).isEqualTo(10);
    }

    @Test
    void step_derivation_clamps_to_points_per_query_cap() {
        // 1 year range, 1s step would exceed 11000 point cap
        TimeSeriesFetchRequest request = new FakeFetchRequest(
                ImmutableMetric.builder().intrinsicTag("name", "foo").build(),
                Instant.ofEpochSecond(0),
                Instant.ofEpochSecond(31_536_000L),
                Duration.ofSeconds(1));
        long step = PrometheusReadClient.stepSeconds(request);
        assertThat(31_536_000L / step).isLessThanOrEqualTo(11_000L);
    }

    // ---------- helpers ----------------------------------------------------

    /** Lightweight in-test TimeSeriesFetchRequest since the API bundle exposes
     *  only the interface. */
    private record FakeFetchRequest(
            Metric metric, Instant start, Instant end, Duration step)
            implements TimeSeriesFetchRequest {

        @Override public Metric getMetric()  { return metric; }
        @Override public Instant getStart()  { return start; }
        @Override public Instant getEnd()    { return end; }
        @Override public Duration getStep()  { return step; }
        @Override public org.opennms.integration.api.v1.timeseries.Aggregation getAggregation() {
            return org.opennms.integration.api.v1.timeseries.Aggregation.NONE;
        }
    }
}
