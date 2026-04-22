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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableDataPoint;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;

/**
 * Parses Prometheus HTTP API responses into OpenNMS TSS domain objects.
 *
 * <p>The two shapes handled:
 * <ul>
 *   <li>{@code /api/v1/series} — a JSON array of label objects, each
 *       reconstructed as a {@link Metric}.</li>
 *   <li>{@code /api/v1/query_range} — a matrix response with
 *       {@code result[].values = [[unix_ts, value_string], ...]} pairs
 *       reconstructed as {@link DataPoint}s.</li>
 * </ul>
 *
 * <p>On the reconstruction side, the plugin is deliberately partition-lossy:
 * {@code __name__} and {@code resourceId} land as intrinsic tags; everything
 * else becomes a meta tag. The round-trip through the query API does not
 * preserve which labels were originally intrinsic/meta/external on the
 * write side.
 */
public final class PromResponseParser {

    private PromResponseParser() {}

    /**
     * Parse a {@code /api/v1/series} response into a list of Metric objects.
     * Every label pair becomes a tag; {@code __name__} becomes intrinsic
     * {@link IntrinsicTagNames#name}; {@code resourceId} becomes intrinsic
     * {@link IntrinsicTagNames#resourceId}; everything else becomes a meta
     * tag.
     */
    public static List<Metric> parseSeriesResponse(String json) throws StorageException {
        try {
            JSONObject root = parseJson(json);
            requireSuccess(root);
            JSONArray data = root.optJSONArray("data");
            if (data == null) return List.of();
            List<Metric> out = new ArrayList<>(data.length());
            for (int i = 0; i < data.length(); i++) {
                JSONObject labels = data.getJSONObject(i);
                out.add(labelObjectToMetric(labels));
            }
            return out;
        } catch (JSONException | IllegalStateException e) {
            throw new StorageException(
                "failed to parse Prometheus /series response: " + e.getMessage(), e);
        }
    }

    /**
     * Parse a {@code /api/v1/query_range} matrix response into a list of
     * DataPoints. When the response contains multiple series (selector was not
     * unique), points from all series are merged in order, with duplicates
     * removed (same-timestamp entries keep the last value — matching the
     * write-side last-write-wins dedup). Prometheus returns non-finite values
     * as the literal strings {@code "NaN"}, {@code "+Inf"}, {@code "-Inf"},
     * which this method translates to {@link Double#NaN} /
     * {@link Double#POSITIVE_INFINITY} / {@link Double#NEGATIVE_INFINITY}.
     */
    public static List<DataPoint> parseRangeResponse(String json) throws StorageException {
        try {
            JSONObject root = parseJson(json);
            requireSuccess(root);
            JSONObject data = root.optJSONObject("data");
            if (data == null) return List.of();
            JSONArray results = data.optJSONArray("result");
            if (results == null || results.isEmpty()) return List.of();

            // Merge datapoints across all series in the matrix. Duplicate
            // timestamps collapse via last-write-wins to match the write-side
            // contract.
            java.util.TreeMap<Long, Double> byTimestamp = new java.util.TreeMap<>();
            for (int r = 0; r < results.length(); r++) {
                JSONArray values = results.getJSONObject(r).optJSONArray("values");
                if (values == null) continue;
                for (int i = 0; i < values.length(); i++) {
                    JSONArray pair = values.getJSONArray(i);
                    double unixTs = pair.getDouble(0);
                    double value  = parsePromValue(pair.getString(1));
                    long millis   = Math.round(unixTs * 1_000.0);
                    byTimestamp.put(millis, value);
                }
            }

            List<DataPoint> out = new ArrayList<>(byTimestamp.size());
            for (java.util.Map.Entry<Long, Double> e : byTimestamp.entrySet()) {
                out.add(new ImmutableDataPoint(Instant.ofEpochMilli(e.getKey()), e.getValue()));
            }
            return out;
        } catch (JSONException | IllegalStateException | NumberFormatException e) {
            throw new StorageException(
                "failed to parse Prometheus /query_range response: " + e.getMessage(), e);
        }
    }

    /** Translate Prometheus's textual form of non-finite doubles into Java's. */
    static double parsePromValue(String raw) {
        if (raw == null) throw new NumberFormatException("null value");
        switch (raw) {
            case "NaN":    return Double.NaN;
            case "+Inf":   return Double.POSITIVE_INFINITY;
            case "-Inf":   return Double.NEGATIVE_INFINITY;
            case "Inf":    return Double.POSITIVE_INFINITY;
            default:       return Double.parseDouble(raw);
        }
    }

    private static Metric labelObjectToMetric(JSONObject labels) {
        ImmutableMetric.MetricBuilder mb = ImmutableMetric.builder();
        for (String key : labels.keySet()) {
            // Use optString so an unexpectedly null / nested-object value
            // becomes an empty string rather than throwing JSONException.
            String value = labels.optString(key, "");
            if ("__name__".equals(key)) {
                mb.intrinsicTag(IntrinsicTagNames.name, value);
            } else if (IntrinsicTagNames.resourceId.equals(key)) {
                mb.intrinsicTag(IntrinsicTagNames.resourceId, value);
            } else {
                mb.metaTag(key, value);
            }
        }
        return mb.build();
    }

    private static JSONObject parseJson(String json) {
        if (json == null || json.isEmpty()) {
            throw new IllegalStateException("empty response body");
        }
        return new JSONObject(json);
    }

    private static void requireSuccess(JSONObject root) {
        String status = root.optString("status", "");
        if (!"success".equals(status)) {
            String err = root.optString("error", "(no error field)");
            throw new IllegalStateException(
                "Prometheus API returned status=" + status + ", error=" + err);
        }
    }
}
