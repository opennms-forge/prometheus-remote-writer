/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;
import org.opennms.plugins.prometheus.remotewriter.wire.proto.WriteRequest;
import org.opennms.plugins.prometheus.remotewriter.wire.v2.proto.Request;
import org.xerial.snappy.Snappy;

/**
 * Cross-version parity for the dedup, non-finite, and ordering invariants.
 * Both v1 and v2 builders must apply the same per-series rules — operator
 * counters and downstream behavior depend on it. If a future change to one
 * builder diverges (e.g. v2 starts dedup'ing on a different key, or shifts
 * to first-write-wins), this test fails loudly instead of silently shipping
 * a counter that means different things on v1 vs v2.
 */
class RemoteWriteV1V2DedupParityTest {

    @Test
    void duplicate_timestamps_dedup_identically_across_versions() throws Exception {
        Map<String, String> labels = labels("__name__", "metric");
        List<MappedSample> input = List.of(
                sample(labels, 1_000L, 1.0),
                sample(labels, 2_000L, 2.0),
                sample(labels, 2_000L, 2.5),  // dup-ts, later wins
                sample(labels, 3_000L, 3.0));

        BuildResult v1 = RemoteWriteRequestBuilder.build(input);
        BuildResult v2 = RemoteWriteV2RequestBuilder.build(input);

        // Counters must agree exactly.
        assertThat(v1.samplesWritten()).isEqualTo(v2.samplesWritten()).isEqualTo(3);
        assertThat(v1.samplesDroppedDuplicate()).isEqualTo(v2.samplesDroppedDuplicate()).isEqualTo(1);
        assertThat(v1.samplesDroppedNonfinite()).isEqualTo(v2.samplesDroppedNonfinite()).isZero();

        // Surviving wire samples (ordered ts:value pairs) must match.
        assertThat(decodeV1(v1)).containsExactly("1000:1.0", "2000:2.5", "3000:3.0");
        assertThat(decodeV2(v2)).containsExactly("1000:1.0", "2000:2.5", "3000:3.0");
    }

    @Test
    void non_finite_drop_counts_match_across_versions() throws Exception {
        List<MappedSample> input = List.of(
                sample(labels("__name__", "ok"), 1L, 1.0),
                sample(labels("__name__", "nan"), 2L, Double.NaN),
                sample(labels("__name__", "pinf"), 3L, Double.POSITIVE_INFINITY),
                sample(labels("__name__", "ninf"), 4L, Double.NEGATIVE_INFINITY));

        BuildResult v1 = RemoteWriteRequestBuilder.build(input);
        BuildResult v2 = RemoteWriteV2RequestBuilder.build(input);

        assertThat(v1.samplesDroppedNonfinite()).isEqualTo(v2.samplesDroppedNonfinite()).isEqualTo(3);
        assertThat(v1.samplesWritten()).isEqualTo(v2.samplesWritten()).isEqualTo(1);
    }

    @Test
    void duplicates_in_different_series_do_not_collapse_in_either_version() {
        // Same timestamp, different label set → two series, both survive.
        List<MappedSample> input = List.of(
                sample(labels("__name__", "foo", "node", "1:1"), 1_000L, 1.0),
                sample(labels("__name__", "foo", "node", "2:2"), 1_000L, 2.0));

        BuildResult v1 = RemoteWriteRequestBuilder.build(input);
        BuildResult v2 = RemoteWriteV2RequestBuilder.build(input);

        assertThat(v1.samplesDroppedDuplicate()).isEqualTo(v2.samplesDroppedDuplicate()).isZero();
        assertThat(v1.samplesWritten()).isEqualTo(v2.samplesWritten()).isEqualTo(2);
    }

    // --- helpers ---------------------------------------------------------------

    /** Decodes a v1 BuildResult to "ts:value" strings in series-then-sample order. */
    private static List<String> decodeV1(BuildResult r) throws Exception {
        WriteRequest req = WriteRequest.parseFrom(Snappy.uncompress(r.compressedPayload()));
        List<String> out = new ArrayList<>();
        for (var ts : req.getTimeseriesList()) {
            for (var s : ts.getSamplesList()) {
                out.add(s.getTimestamp() + ":" + s.getValue());
            }
        }
        return out;
    }

    /** Decodes a v2 BuildResult to "ts:value" strings in series-then-sample order. */
    private static List<String> decodeV2(BuildResult r) throws Exception {
        Request req = Request.parseFrom(Snappy.uncompress(r.compressedPayload()));
        List<String> out = new ArrayList<>();
        for (var ts : req.getTimeseriesList()) {
            for (var s : ts.getSamplesList()) {
                out.add(s.getTimestamp() + ":" + s.getValue());
            }
        }
        return out;
    }

    private static MappedSample sample(Map<String, String> labels, long ts, double value) {
        return new MappedSample(new LinkedHashMap<>(labels), ts, value);
    }

    private static Map<String, String> labels(String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("labels(...) requires even arg count");
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put(pairs[i], pairs[i + 1]);
        }
        return out;
    }
}
