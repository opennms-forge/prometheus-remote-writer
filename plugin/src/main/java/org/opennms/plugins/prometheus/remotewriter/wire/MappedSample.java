/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import java.util.Map;
import java.util.Objects;

/**
 * One Prometheus-shaped sample — already reduced from an OpenNMS
 * {@code org.opennms.integration.api.v1.timeseries.Sample} by the label
 * mapper (wired in task 4.x). The {@code labels} map may be in any order;
 * the wire serializer sorts lexicographically by name before emission.
 *
 * @param labels       label name → value (order-insensitive)
 * @param timestampMs  epoch-millis timestamp
 * @param value        sample value; non-finite values are dropped by the
 *                     serializer ({@link RemoteWriteRequestBuilder})
 */
public record MappedSample(Map<String, String> labels, long timestampMs, double value) {

    /** Prometheus reserves this label name for the metric name; every series must have it. */
    public static final String METRIC_NAME_LABEL = "__name__";

    public MappedSample {
        Objects.requireNonNull(labels, "labels");
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("labels must not be empty");
        }
        if (!labels.containsKey(METRIC_NAME_LABEL)) {
            throw new IllegalArgumentException(
                "labels must contain " + METRIC_NAME_LABEL
                    + " — Prometheus rejects series without a metric name");
        }
    }
}
