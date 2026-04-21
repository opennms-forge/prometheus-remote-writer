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
import org.json.JSONObject;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
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
    public static List<Metric> parseSeriesResponse(String json) {
        JSONObject root = new JSONObject(json);
        requireSuccess(root);
        JSONArray data = root.optJSONArray("data");
        if (data == null) return List.of();
        List<Metric> out = new ArrayList<>(data.length());
        for (int i = 0; i < data.length(); i++) {
            JSONObject labels = data.getJSONObject(i);
            out.add(labelObjectToMetric(labels));
        }
        return out;
    }

    /**
     * Parse a {@code /api/v1/query_range} matrix response into a list of
     * DataPoints. The matrix is expected to contain a single result; with
     * multiple results only the first is returned.
     */
    public static List<DataPoint> parseRangeResponse(String json) {
        JSONObject root = new JSONObject(json);
        requireSuccess(root);
        JSONObject data = root.optJSONObject("data");
        if (data == null) return List.of();
        JSONArray results = data.optJSONArray("result");
        if (results == null || results.isEmpty()) return List.of();
        JSONArray values = results.getJSONObject(0).optJSONArray("values");
        if (values == null) return List.of();

        List<DataPoint> out = new ArrayList<>(values.length());
        for (int i = 0; i < values.length(); i++) {
            JSONArray pair = values.getJSONArray(i);
            double unixTs = pair.getDouble(0);
            double value  = Double.parseDouble(pair.getString(1));
            long millis   = (long) (unixTs * 1_000);
            out.add(new ImmutableDataPoint(Instant.ofEpochMilli(millis), value));
        }
        return out;
    }

    private static Metric labelObjectToMetric(JSONObject labels) {
        ImmutableMetric.MetricBuilder mb = ImmutableMetric.builder();
        for (String key : labels.keySet()) {
            String value = labels.getString(key);
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

    private static void requireSuccess(JSONObject root) {
        String status = root.optString("status", "");
        if (!"success".equals(status)) {
            String err = root.optString("error", "(no error field)");
            throw new IllegalStateException(
                "Prometheus API returned status=" + status + ", error=" + err);
        }
    }
}
