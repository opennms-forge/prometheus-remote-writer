/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.read;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesData;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.TlsConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

/**
 * Read-side client that talks to the Prometheus HTTP query API. Implements
 * the two SPI queries ({@code findMetrics}, {@code getTimeSeriesData}) as
 * {@code GET /api/v1/series} and {@code GET /api/v1/query_range} calls.
 *
 * <p>Shares authentication, tenant, and TLS settings with the write client;
 * each call attaches the same headers so a single credential / tenant
 * configuration covers both directions.
 */
public final class PrometheusReadClient {

    private static final int MAX_POINTS_PER_RANGE = 11_000;
    /** Cap on the bytes we'll read from a single Prom-API response.
     *  Protects the plugin from a misbehaving backend returning a
     *  multi-megabyte error page or a pathologically large matrix. */
    private static final long MAX_RESPONSE_BYTES = 8L * 1024 * 1024; // 8 MiB

    private final OkHttpClient http;
    private final PrometheusRemoteWriterConfig config;
    private final MtypeFallback mtypeFallback;

    /** Test-friendly constructor — no metrics sink, so the synthesis counter
     *  is not driven. Production code uses the two-arg constructor. */
    public PrometheusReadClient(PrometheusRemoteWriterConfig config) {
        this(config, null);
    }

    public PrometheusReadClient(PrometheusRemoteWriterConfig config, PluginMetrics metrics) {
        this.config = Objects.requireNonNull(config);
        OkHttpClient.Builder b = new OkHttpClient.Builder()
                .connectTimeout(config.getHttpConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getHttpReadTimeoutMs(),       TimeUnit.MILLISECONDS)
                .writeTimeout(config.getHttpWriteTimeoutMs(),     TimeUnit.MILLISECONDS);
        TlsConfig.configure(b, config);
        this.http = b.build();
        this.mtypeFallback = new MtypeFallback(metrics);
    }

    /** Visible for tests — exposes the WARN-tracking set so tests can assert
     *  one-shot semantics and LRU eviction without depending on a logging
     *  framework. */
    java.util.Set<String> warnedMtypeMetricsForTesting() {
        return mtypeFallback.warnedMetricsForTesting();
    }

    public void shutdown() {
        java.util.concurrent.ExecutorService exec = http.dispatcher().executorService();
        exec.shutdown();
        try {
            if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                exec.shutdownNow();
                exec.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            exec.shutdownNow();
            Thread.currentThread().interrupt();
        }
        http.connectionPool().evictAll();
        if (config.isTlsInsecureSkipVerify()) {
            org.opennms.plugins.prometheus.remotewriter.http.TlsConfig.stopInsecureWarn();
        }
    }

    /** Translates a matcher collection to a {@code /api/v1/series} call. */
    public List<Metric> findMetrics(Collection<TagMatcher> matchers) throws StorageException {
        if (matchers == null) {
            throw new NullPointerException("matchers");
        }
        if (matchers.isEmpty()) {
            throw new IllegalArgumentException("matchers must not be empty");
        }
        String selector = PromQLBuilder.fromMatchers(matchers);
        long startEpoch = Instant.now().getEpochSecond() - config.getMaxSeriesLookbackSeconds();

        String url = config.getReadUrl()
                + "/api/v1/series?match[]=" + urlEncode(selector)
                + "&start=" + startEpoch;
        String body = executeGet(url);
        List<Metric> raw = PromResponseParser.parseSeriesResponse(body);
        // Apply the mtype fallback per-Metric — the read path's contract with
        // OpenNMS-core requires every returned Metric carry an mtype meta tag.
        List<Metric> out = new java.util.ArrayList<>(raw.size());
        for (Metric m : raw) out.add(mtypeFallback.apply(m));
        return out;
    }

    /** Translates a fetch request to a {@code /api/v1/query_range} call. */
    public TimeSeriesData getTimeSeriesData(TimeSeriesFetchRequest request) throws StorageException {
        Objects.requireNonNull(request, "request");
        Metric requestMetric = Objects.requireNonNull(request.getMetric(), "request.metric");
        String selector = PromQLBuilder.fromIntrinsicTags(requestMetric.getIntrinsicTags());
        long startSec = request.getStart().getEpochSecond();
        long endSec   = request.getEnd().getEpochSecond();
        long stepSec  = stepSeconds(request);

        String url = config.getReadUrl()
                + "/api/v1/query_range?query=" + urlEncode(selector)
                + "&start=" + startSec
                + "&end=" + endSec
                + "&step=" + stepSec + "s";

        String body = executeGet(url);
        // Reconstruct the Metric from the response's actual labels (where
        // mtype lives), not from the request Metric (which OpenNMS builds
        // with only resourceId+name — no mtype). When the matrix is empty,
        // skip the synthesis fallback altogether: OpenNMS streams the
        // empty datapoint list and never dereferences mtype on it, so a
        // synthesis tick here would just inflate the counter for graphs
        // that won't render anyway.
        PromResponseParser.RangeResult result =
                PromResponseParser.parseRangeResponseWithMetric(body, requestMetric);
        Metric metric = result.points().isEmpty()
                ? result.metric()
                : mtypeFallback.apply(result.metric());
        return new ImmutableTimeSeriesData(metric, result.points());
    }

    /**
     * Derive a step in seconds. Uses the request's explicit step if given;
     * otherwise falls back to {@code (end - start) / 600}, clamped to at
     * least 1s and at most a value that would exceed Prometheus's 11 000
     * points-per-query ceiling.
     *
     * <p>Overflow-safe: computes {@code end - start} with
     * {@link Math#subtractExact}. A range that wraps {@code Long.MAX_VALUE}
     * is rejected with {@link IllegalArgumentException}, which surfaces to
     * the caller rather than producing a garbage step that Prometheus will
     * 422 on.
     */
    static long stepSeconds(TimeSeriesFetchRequest request) {
        Duration explicit = request.getStep();
        long start = request.getStart().getEpochSecond();
        long end   = request.getEnd().getEpochSecond();
        long rangeSec;
        try {
            rangeSec = Math.max(1, Math.subtractExact(end, start));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                "fetch-request range overflows Long: start=" + start + ", end=" + end, overflow);
        }

        long step = explicit != null && !explicit.isZero()
                ? Math.max(1, explicit.getSeconds())
                : Math.max(1, rangeSec / 600);

        long minStepForPointsCap = (rangeSec + MAX_POINTS_PER_RANGE - 1) / MAX_POINTS_PER_RANGE;
        if (step < minStepForPointsCap) step = minStepForPointsCap;
        return step;
    }

    // -- HTTP ---------------------------------------------------------------

    private String executeGet(String url) throws StorageException {
        Request.Builder rb = new Request.Builder().url(url).get();
        if (config.hasBasicAuth()) {
            String creds = config.getBasicUsername() + ":" + config.getBasicPassword();
            rb.addHeader("Authorization", "Basic "
                + java.util.Base64.getEncoder()
                    .encodeToString(creds.getBytes(StandardCharsets.UTF_8)));
        } else if (config.hasBearerAuth()) {
            rb.addHeader("Authorization", "Bearer " + config.getBearerToken());
        }
        if (config.hasTenant()) {
            rb.addHeader("X-Scope-OrgID", config.getTenantOrgId());
        }

        try (Response resp = http.newCall(rb.build()).execute()) {
            String text = readBodyCapped(resp);
            if (!resp.isSuccessful()) {
                throw new StorageException(
                    "Prometheus query failed: " + resp.code() + " " + resp.message()
                        + " — " + text);
            }
            return text;
        } catch (IOException e) {
            throw new StorageException("Prometheus query transport error: " + e.getMessage(), e);
        }
    }

    /** Read the response body, capped at {@link #MAX_RESPONSE_BYTES} to
     *  protect against a misbehaving backend returning a huge payload.
     *  When the cap is exceeded, throws a {@link StorageException} rather
     *  than silently truncating — a truncated JSON would parse as
     *  malformed and produce a less actionable error. */
    private static String readBodyCapped(Response resp) throws IOException, StorageException {
        ResponseBody body = resp.body();
        if (body == null) return "";
        long declared = body.contentLength();
        if (declared > MAX_RESPONSE_BYTES) {
            throw new StorageException(
                "Prometheus response too large: " + declared + " bytes (cap "
                    + MAX_RESPONSE_BYTES + ")");
        }
        try (okio.BufferedSource src = body.source()) {
            if (!src.request(MAX_RESPONSE_BYTES + 1)) {
                // fits within the cap — read it all
                return body.string();
            }
            throw new StorageException(
                "Prometheus response exceeded cap of " + MAX_RESPONSE_BYTES + " bytes");
        }
    }

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
