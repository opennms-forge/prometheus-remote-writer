/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

class LabelMapperTest {

    private static final LabelMapper DEFAULT_MAPPER = new LabelMapper(defaultConfig());

    // ---------- defaults ----------------------------------------------------

    @Test
    void emits_name_and_resource_id_from_intrinsic_tags() {
        Sample s = interfaceSample();
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out).isNotNull();
        assertThat(out.labels()).containsEntry("__name__", "ifHCInOctets");
        assertThat(out.labels()).containsEntry("resourceId", "nodeSource[NOC:router-42].interfaceSnmp[eth0]");
    }

    @Test
    void emits_parsed_resource_components() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("resource_type", "interfaceSnmp");
        assertThat(out.labels()).containsEntry("resource_instance", "eth0");
    }

    @Test
    void node_uses_fs_qualified_identity_when_available() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("node", "NOC:router-42");
        assertThat(out.labels()).containsEntry("foreign_source", "NOC");
        assertThat(out.labels()).containsEntry("foreign_id", "router-42");
    }

    @Test
    void node_falls_back_to_parsed_id_when_no_foreign_source_is_present() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "node[42].interfaceSnmp[eth0]")
                .build());
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "42");
    }

    @Test
    void node_falls_back_to_node_id_tag_when_no_resource_id() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .externalTag("nodeId", "7")
                .build());
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("node", "7");
    }

    @Test
    void unparseable_resource_id_emits_only_raw_label() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .intrinsicTag("resourceId", "not-a-resource-id")
                .build());
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsEntry("resourceId", "not-a-resource-id");
        assertThat(out.labels()).doesNotContainKey("resource_type");
        assertThat(out.labels()).doesNotContainKey("resource_instance");
    }

    @Test
    void node_label_and_location_emitted_when_present() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("node_label", "router-42.example.com");
        assertThat(out.labels()).containsEntry("location", "default");
    }

    @Test
    void if_name_and_if_descr_emitted_when_present() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("if_name", "eth0");
        assertThat(out.labels()).containsEntry("if_descr", "GigabitEthernet0/0");
    }

    @Test
    void if_speed_normalizes_high_speed_to_bits_per_second() {
        // ifHighSpeed=1000 megabits = 1e9 bits/s
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("if_speed", "1000000000");
    }

    @Test
    void categories_expanded_one_label_per_name() {
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).containsEntry("onms_cat_Routers", "true");
        assertThat(out.labels()).containsEntry("onms_cat_ProductionSites", "true");
    }

    @Test
    void category_names_with_forbidden_chars_are_sanitized() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("name", "foo")
                .externalTag("categories", "Server Room-B, Front.Office"));
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.labels()).containsKey("onms_cat_Server_Room_B");
        assertThat(out.labels()).containsKey("onms_cat_Front_Office");
    }

    @Test
    void returns_null_when_metric_has_no_name() {
        Sample s = sample(ImmutableMetric.builder()
                .intrinsicTag("resourceId", "node[1].interfaceSnmp[eth0]")
                .externalTag("nodeId", "1"));
        assertThat(DEFAULT_MAPPER.map(s)).isNull();
    }

    @Test
    void metric_prefix_prepended_when_configured() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setMetricPrefix("onms_");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("__name__", "onms_ifHCInOctets");
    }

    @Test
    void timestamp_and_value_round_trip_from_sample() {
        Instant when = Instant.ofEpochMilli(1_742_000_000L);
        Sample s = ImmutableSample.builder()
                .metric(ImmutableMetric.builder()
                        .intrinsicTag("name", "foo")
                        .intrinsicTag("resourceId", "node[1].nodeSnmp[]")
                        .build())
                .time(when)
                .value(3.14)
                .build();
        MappedSample out = DEFAULT_MAPPER.map(s);
        assertThat(out.timestampMs()).isEqualTo(when.toEpochMilli());
        assertThat(out.value()).isEqualTo(3.14);
    }

    // ---------- override globs ----------------------------------------------

    @Test
    void exclude_glob_removes_default_label() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsExclude("node_label");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("node_label");
        // Other defaults still present.
        assertThat(out.labels()).containsKey("node");
    }

    @Test
    void exclude_glob_with_wildcard_removes_matching_labels() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsExclude("onms_cat_*");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("onms_cat_Routers");
        assertThat(out.labels()).doesNotContainKey("onms_cat_ProductionSites");
    }

    @Test
    void include_glob_surfaces_non_default_tag_snake_cased() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("ifAlias");
        Sample s = interfaceSampleWith("ifAlias", "uplink-to-core");
        MappedSample out = new LabelMapper(c).map(s);
        // Matches the default allowlist's convention (ifName -> if_name).
        assertThat(out.labels()).containsEntry("if_alias", "uplink-to-core");
    }

    @Test
    void include_glob_does_not_overwrite_default_label() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        // 'ifName' is a source tag key AND the default emits 'if_name'.
        // Include match on 'ifName' must not clobber the default value.
        c.setLabelsInclude("ifName");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        // Default label from ifName source.
        assertThat(out.labels()).containsEntry("if_name", "eth0");
    }

    // ---------- metadata passthrough (integration with LabelMapper) ---------

    @Test
    void metadata_is_emitted_alongside_defaults_when_enabled() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setMetadataEnabled(true);
        c.setMetadataInclude("requisition:*");
        Sample s = interfaceSampleWith("requisition:location", "Pittsboro");
        MappedSample out = new LabelMapper(c).map(s);
        assertThat(out.labels()).containsEntry("onms_meta_requisition_location", "Pittsboro");
        // Defaults still present.
        assertThat(out.labels()).containsKey("__name__");
        assertThat(out.labels()).containsKey("node");
    }

    @Test
    void metadata_denylist_blocks_reflect_in_counter() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setMetadataEnabled(true);
        c.setMetadataInclude("requisition:*");
        LabelMapper mapper = new LabelMapper(c);
        Sample s = interfaceSampleWith("requisition:snmp-community", "public");
        MappedSample out = mapper.map(s);
        assertThat(out.labels()).doesNotContainKey("onms_meta_requisition_snmp_community");
        assertThat(mapper.getMetadataDenylistBlockedCount()).isEqualTo(1);
    }

    @Test
    void rename_applies_after_include_and_exclude() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsRename("node_label->hostname, location->region");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("hostname", "router-42.example.com");
        assertThat(out.labels()).containsEntry("region", "default");
        assertThat(out.labels()).doesNotContainKey("node_label");
        assertThat(out.labels()).doesNotContainKey("location");
    }

    // ---------- fixtures ----------------------------------------------------

    /** A well-populated interface sample: FS-qualified node, 2 categories, 1 Gbps. */
    private static Sample interfaceSample() {
        return interfaceSampleWith(null, null);
    }

    private static Sample interfaceSampleWith(String extraKey, String extraValue) {
        ImmutableMetric.MetricBuilder mb = ImmutableMetric.builder()
                .intrinsicTag("name", "ifHCInOctets")
                .intrinsicTag("resourceId", "nodeSource[NOC:router-42].interfaceSnmp[eth0]")
                .externalTag("nodeLabel", "router-42.example.com")
                .externalTag("foreignSource", "NOC")
                .externalTag("foreignId", "router-42")
                .externalTag("location", "default")
                .externalTag("ifName", "eth0")
                .externalTag("ifDescr", "GigabitEthernet0/0")
                .externalTag("ifHighSpeed", "1000")
                .externalTag("ifSpeed", "4294967295")
                .externalTag("categories", "Routers, ProductionSites");
        if (extraKey != null) {
            mb.externalTag(extraKey, extraValue);
        }
        return sample(mb);
    }

    private static Sample sample(ImmutableMetric.MetricBuilder metricBuilder) {
        return sample(metricBuilder.build());
    }

    private static Sample sample(org.opennms.integration.api.v1.timeseries.Metric m) {
        return ImmutableSample.builder()
                .metric(m)
                .time(Instant.ofEpochMilli(1_000_000L))
                .value(42.0)
                .build();
    }

    private static PrometheusRemoteWriterConfig defaultConfig() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        return c;
    }
}
