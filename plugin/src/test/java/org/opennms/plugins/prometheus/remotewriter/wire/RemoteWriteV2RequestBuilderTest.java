/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;
import org.opennms.plugins.prometheus.remotewriter.wire.v2.proto.Request;
import org.opennms.plugins.prometheus.remotewriter.wire.v2.proto.TimeSeries;
import org.xerial.snappy.Snappy;

class RemoteWriteV2RequestBuilderTest {

    @Test
    void empty_input_produces_only_the_empty_symbol() throws Exception {
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of());
        Request req = decode(r);
        assertThat(req.getSymbolsList()).containsExactly("");
        assertThat(req.getTimeseriesCount()).isZero();
        assertThat(r.samplesWritten()).isZero();
    }

    @Test
    void single_sample_produces_three_symbols_and_one_series() throws Exception {
        MappedSample s = sample(Map.of("__name__", "up"), 1_000L, 1.0);
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(s));
        Request req = decode(r);

        // symbols[0] = "" + "__name__" + "up"
        assertThat(req.getSymbolsList()).containsExactly("", "__name__", "up");
        assertThat(req.getTimeseriesCount()).isEqualTo(1);
        TimeSeries ts = req.getTimeseries(0);
        assertThat(ts.getLabelsRefsList()).containsExactly(1, 2);
        assertThat(ts.getSamples(0).getValue()).isEqualTo(1.0);
        assertThat(ts.getSamples(0).getTimestamp()).isEqualTo(1_000L);
    }

    @Test
    void empty_string_is_at_symbols_index_zero() throws Exception {
        // Spec invariant: symbols[0] SHALL be "". Pin it explicitly.
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(
                sample(Map.of("__name__", "x"), 0L, 0.0)));
        Request req = decode(r);
        assertThat(req.getSymbols(0)).isEmpty();
    }

    @Test
    void two_series_sharing_label_names_intern_each_name_once() throws Exception {
        Map<String, String> a = labels("__name__", "metric_a", "node", "n1", "job", "snmp");
        Map<String, String> b = labels("__name__", "metric_b", "node", "n1", "job", "snmp");
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(
                sample(a, 1L, 1.0),
                sample(b, 1L, 2.0)));
        Request req = decode(r);

        // Symbols expected (insertion-deterministic, label-sorted-keys
        // means: __name__, job, node — in that order — interned first):
        //   "", "__name__", "metric_a", "job", "snmp", "node", "n1", "metric_b"
        assertThat(req.getSymbolsList()).containsExactly(
                "", "__name__", "metric_a", "job", "snmp", "node", "n1", "metric_b");
        // Each name and the shared values appear exactly once.
        long jobOccurrences = req.getSymbolsList().stream().filter("job"::equals).count();
        long nodeOccurrences = req.getSymbolsList().stream().filter("node"::equals).count();
        long snmpOccurrences = req.getSymbolsList().stream().filter("snmp"::equals).count();
        long n1Occurrences = req.getSymbolsList().stream().filter("n1"::equals).count();
        assertThat(jobOccurrences).isEqualTo(1);
        assertThat(nodeOccurrences).isEqualTo(1);
        assertThat(snmpOccurrences).isEqualTo(1);
        assertThat(n1Occurrences).isEqualTo(1);
    }

    @Test
    void labels_refs_decode_to_original_label_set() throws Exception {
        Map<String, String> labels = labels(
                "__name__", "ifHCInOctets",
                "node", "NOC:42",
                "job", "snmp",
                "instance", "NOC:42",
                "if_name", "eth0");
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(
                sample(labels, 1_000L, 42.0)));
        Request req = decode(r);

        // Reconstruct the label set from labels_refs + symbols.
        TimeSeries ts = req.getTimeseries(0);
        Map<String, String> reconstructed = new LinkedHashMap<>();
        List<Integer> refs = ts.getLabelsRefsList();
        assertThat(refs.size() % 2).isEqualTo(0);
        for (int i = 0; i < refs.size(); i += 2) {
            String name = req.getSymbols(refs.get(i));
            String value = req.getSymbols(refs.get(i + 1));
            reconstructed.put(name, value);
        }
        assertThat(reconstructed).containsExactlyInAnyOrderEntriesOf(labels);
    }

    @Test
    void round_trip_via_decode_re_encode_is_equal_by_value() throws Exception {
        // Two builds of the same input produce equal Request messages.
        // We use round-trip equality (parse → equals) rather than byte
        // equality to stay resilient to protobuf-java map iteration
        // changes — same lesson learned from WalEntryCodecTest.
        MappedSample s1 = sample(labels("__name__", "x", "a", "1"), 1L, 1.0);
        MappedSample s2 = sample(labels("__name__", "y", "a", "1"), 2L, 2.0);
        BuildResult first = RemoteWriteV2RequestBuilder.build(List.of(s1, s2));
        BuildResult second = RemoteWriteV2RequestBuilder.build(List.of(s1, s2));
        assertThat(decode(first)).isEqualTo(decode(second));
    }

    @Test
    void non_finite_values_are_dropped() throws Exception {
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(
                sample(Map.of("__name__", "ok"), 1L, 1.0),
                sample(Map.of("__name__", "nan"), 2L, Double.NaN),
                sample(Map.of("__name__", "pinf"), 3L, Double.POSITIVE_INFINITY),
                sample(Map.of("__name__", "ninf"), 4L, Double.NEGATIVE_INFINITY)));
        assertThat(r.samplesDroppedNonfinite()).isEqualTo(3);
        assertThat(r.samplesWritten()).isEqualTo(1);
    }

    @Test
    void duplicate_timestamps_within_a_series_are_collapsed() throws Exception {
        Map<String, String> labels = Map.of("__name__", "metric");
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(
                sample(labels, 1_000L, 1.0),
                sample(labels, 1_000L, 2.0), // same timestamp — earlier dropped, later kept
                sample(labels, 2_000L, 3.0)));
        assertThat(r.samplesDroppedDuplicate()).isEqualTo(1);
        assertThat(r.samplesWritten()).isEqualTo(2);

        // Verify only the last-write-wins survivor is on the wire.
        Request req = decode(r);
        TimeSeries ts = req.getTimeseries(0);
        assertThat(ts.getSamplesCount()).isEqualTo(2);
        assertThat(ts.getSamples(0).getValue()).isEqualTo(2.0); // dedup keeps later
        assertThat(ts.getSamples(0).getTimestamp()).isEqualTo(1_000L);
        assertThat(ts.getSamples(1).getValue()).isEqualTo(3.0);
    }

    @Test
    void payload_is_snappy_encoded_not_raw_protobuf() throws Exception {
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(
                sample(Map.of("__name__", "x"), 1L, 1.0)));
        // Decompress + parse should succeed; parsing the compressed
        // bytes directly as protobuf should not produce the same Request.
        byte[] decompressed = Snappy.uncompress(r.compressedPayload());
        Request fromDecompressed = Request.parseFrom(decompressed);
        assertThat(fromDecompressed.getTimeseriesCount()).isEqualTo(1);
        assertThat(decompressed.length).isEqualTo(r.uncompressedSize());
    }

    @Test
    void samples_within_series_are_timestamp_ordered_on_the_wire() throws Exception {
        Map<String, String> labels = Map.of("__name__", "metric");
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(
                sample(labels, 3_000L, 3.0),
                sample(labels, 1_000L, 1.0),
                sample(labels, 2_000L, 2.0)));
        Request req = decode(r);
        TimeSeries ts = req.getTimeseries(0);
        assertThat(ts.getSamples(0).getTimestamp()).isEqualTo(1_000L);
        assertThat(ts.getSamples(1).getTimestamp()).isEqualTo(2_000L);
        assertThat(ts.getSamples(2).getTimestamp()).isEqualTo(3_000L);
    }

    @Test
    void labels_refs_size_is_always_even() throws Exception {
        BuildResult r = RemoteWriteV2RequestBuilder.build(List.of(
                sample(labels("__name__", "x", "a", "1", "b", "2"), 1L, 1.0)));
        Request req = decode(r);
        for (TimeSeries ts : req.getTimeseriesList()) {
            assertThat(ts.getLabelsRefsCount() % 2).isEqualTo(0);
        }
    }

    // --- helpers ---------------------------------------------------------------

    private static Request decode(BuildResult r) throws Exception {
        return Request.parseFrom(Snappy.uncompress(r.compressedPayload()));
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
