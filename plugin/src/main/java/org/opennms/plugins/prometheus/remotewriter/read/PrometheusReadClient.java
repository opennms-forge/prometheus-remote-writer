/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesData;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesData;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.http.TlsConfig;

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

    private final OkHttpClient http;
    private final PrometheusRemoteWriterConfig config;

    public PrometheusReadClient(PrometheusRemoteWriterConfig config) {
        this.config = Objects.requireNonNull(config);
        OkHttpClient.Builder b = new OkHttpClient.Builder()
                .connectTimeout(config.getHttpConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getHttpReadTimeoutMs(),       TimeUnit.MILLISECONDS)
                .writeTimeout(config.getHttpWriteTimeoutMs(),     TimeUnit.MILLISECONDS);
        TlsConfig.configure(b, config);
        this.http = b.build();
    }

    public void shutdown() {
        http.dispatcher().executorService().shutdown();
        http.connectionPool().evictAll();
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
        return PromResponseParser.parseSeriesResponse(body);
    }

    /** Translates a fetch request to a {@code /api/v1/query_range} call. */
    public TimeSeriesData getTimeSeriesData(TimeSeriesFetchRequest request) throws StorageException {
        Objects.requireNonNull(request, "request");
        Metric metric = Objects.requireNonNull(request.getMetric(), "request.metric");
        String selector = PromQLBuilder.fromIntrinsicTags(metric.getIntrinsicTags());
        long startSec = request.getStart().getEpochSecond();
        long endSec   = request.getEnd().getEpochSecond();
        long stepSec  = stepSeconds(request);

        String url = config.getReadUrl()
                + "/api/v1/query_range?query=" + urlEncode(selector)
                + "&start=" + startSec
                + "&end=" + endSec
                + "&step=" + stepSec + "s";

        String body = executeGet(url);
        List<DataPoint> points = PromResponseParser.parseRangeResponse(body);
        return new ImmutableTimeSeriesData(metric, points);
    }

    /**
     * Derive a step in seconds. Uses the request's explicit step if given;
     * otherwise falls back to {@code (end - start) / 600}, clamped to at
     * least 1s and at most a value that would exceed Prometheus's 11 000
     * points-per-query ceiling.
     */
    static long stepSeconds(TimeSeriesFetchRequest request) {
        Duration explicit = request.getStep();
        long start = request.getStart().getEpochSecond();
        long end   = request.getEnd().getEpochSecond();
        long rangeSec = Math.max(1, end - start);

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

        try (Response resp = http.newCall(rb.build()).execute();
             ResponseBody body = resp.body()) {
            String text = body != null ? body.string() : "";
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

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
