/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteOutcome;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteResult;

class RemoteWriteHttpClientTest {

    private static final byte[] PAYLOAD = new byte[] { 1, 2, 3, 4 };

    private MockWebServer server;
    private RemoteWriteHttpClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) client.shutdown();
        server.shutdown();
    }

    // ---------- success + headers ------------------------------------------

    @Test
    void success_emits_remote_write_v1_headers() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        client = newClient(cfg(server));

        WriteResult r = client.write(PAYLOAD);

        assertThat(r.outcome()).isEqualTo(WriteOutcome.SUCCESS);
        assertThat(r.attemptsMade()).isEqualTo(1);
        assertThat(client.getWritesSuccessful()).isEqualTo(1);
        assertThat(client.getBytesWritten()).isEqualTo(PAYLOAD.length);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getMethod()).isEqualTo("POST");
        assertThat(req.getHeader("Content-Type")).isEqualTo("application/x-protobuf");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        assertThat(req.getHeader("X-Prometheus-Remote-Write-Version")).isEqualTo("0.1.0");
        assertThat(req.getHeader("User-Agent")).startsWith("org.opennms.plugins.prometheus-remote-writer/");
        assertThat(req.getBody().readByteArray()).isEqualTo(PAYLOAD);
    }

    @Test
    void v2_headers_use_proto_qualifier_and_version_2_0_0() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setWireProtocolVersion("2");
        client = newClient(c);

        client.write(PAYLOAD);

        RecordedRequest req = server.takeRequest();
        // Content-Type carries the proto= qualifier per the v2 spec —
        // OkHttp may add a charset suffix; assert prefix.
        assertThat(req.getHeader("Content-Type"))
                .startsWith("application/x-protobuf;proto=io.prometheus.write.v2.Request");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        assertThat(req.getHeader("X-Prometheus-Remote-Write-Version")).isEqualTo("2.0.0");
    }

    // ---------- auth permutations ------------------------------------------

    @Test
    void basic_auth_header_is_attached() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setBasicUsername("alice");
        c.setBasicPassword("s3cret");
        client = newClient(c);

        client.write(PAYLOAD);

        String expected = "Basic " + Base64.getEncoder()
                .encodeToString("alice:s3cret".getBytes(StandardCharsets.UTF_8));
        assertThat(server.takeRequest().getHeader("Authorization")).isEqualTo(expected);
    }

    @Test
    void bearer_auth_header_is_attached() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setBearerToken("tok-abc");
        client = newClient(c);

        client.write(PAYLOAD);

        assertThat(server.takeRequest().getHeader("Authorization")).isEqualTo("Bearer tok-abc");
    }

    @Test
    void no_auth_header_when_none_configured() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        client = newClient(cfg(server));
        client.write(PAYLOAD);

        assertThat(server.takeRequest().getHeader("Authorization")).isNull();
    }

    @Test
    void tenant_header_is_attached_when_configured() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        PrometheusRemoteWriterConfig c = cfg(server);
        c.setTenantOrgId("team-a");
        client = newClient(c);

        client.write(PAYLOAD);

        assertThat(server.takeRequest().getHeader("X-Scope-OrgID")).isEqualTo("team-a");
    }

    // ---------- 4xx drop ---------------------------------------------------

    @Test
    void four_xx_drops_and_does_not_retry() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(400).setBody("bad labels"));
        client = newClient(cfg(server));

        WriteResult r = client.write(PAYLOAD);

        assertThat(r.outcome()).isEqualTo(WriteOutcome.DROPPED_4XX);
        assertThat(r.httpStatus()).isEqualTo(400);
        assertThat(r.attemptsMade()).isEqualTo(1);
        assertThat(r.detail()).contains("bad labels");
        assertThat(client.getWrites4xx()).isEqualTo(1);
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    // ---------- 5xx retry --------------------------------------------------

    @Test
    void five_xx_then_success_retries_until_accepted() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(204));
        client = newClient(fastRetry(cfg(server)));

        WriteResult r = client.write(PAYLOAD);

        assertThat(r.outcome()).isEqualTo(WriteOutcome.SUCCESS);
        assertThat(r.attemptsMade()).isEqualTo(3);
        assertThat(server.getRequestCount()).isEqualTo(3);
        assertThat(client.getWritesSuccessful()).isEqualTo(1);
    }

    @Test
    void five_xx_exhausted_reports_dropped() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("oops"));
        server.enqueue(new MockResponse().setResponseCode(500).setBody("oops"));
        server.enqueue(new MockResponse().setResponseCode(500).setBody("oops"));
        PrometheusRemoteWriterConfig c = fastRetry(cfg(server));
        c.setRetryMaxAttempts(3);
        client = newClient(c);

        WriteResult r = client.write(PAYLOAD);

        assertThat(r.outcome()).isEqualTo(WriteOutcome.DROPPED_5XX_EXHAUSTED);
        assertThat(r.httpStatus()).isEqualTo(500);
        assertThat(r.attemptsMade()).isEqualTo(3);
        assertThat(client.getWrites5xxExhausted()).isEqualTo(1);
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    // ---------- helpers ----------------------------------------------------

    private static PrometheusRemoteWriterConfig cfg(MockWebServer server) {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl(server.url("/api/v1/push").toString());
        c.setReadUrl(server.url("/prometheus").toString());
        return c;
    }

    /** Drop retry backoff to sub-millisecond so tests run fast. */
    private static PrometheusRemoteWriterConfig fastRetry(PrometheusRemoteWriterConfig c) {
        c.setRetryInitialBackoffMs(1);
        c.setRetryMaxBackoffMs(2);
        c.setRetryMaxAttempts(5);
        return c;
    }

    private static RemoteWriteHttpClient newClient(PrometheusRemoteWriterConfig c) {
        c.validate();
        return new RemoteWriteHttpClient(c);
    }
}
