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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    // ---------- onms_instance_id --------------------------------------------

    @Test
    void onms_instance_id_is_emitted_when_instance_id_is_set() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("opennms-us-east");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("onms_instance_id", "opennms-us-east");
    }

    @Test
    void onms_instance_id_is_absent_when_instance_id_is_unset() {
        // The default fixture config doesn't set instance.id.
        MappedSample out = DEFAULT_MAPPER.map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("onms_instance_id");
        // Other defaults still emitted.
        assertThat(out.labels()).containsKey("__name__");
        assertThat(out.labels()).containsKey("node");
    }

    @Test
    void onms_instance_id_is_absent_when_instance_id_is_whitespace() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("   ");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("onms_instance_id");
    }

    @Test
    void onms_instance_id_honors_labels_exclude() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("opennms-us-east");
        c.setLabelsExclude("onms_instance_id");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).doesNotContainKey("onms_instance_id");
    }

    @Test
    void onms_instance_id_honors_labels_rename() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("opennms-us-east");
        c.setLabelsRename("onms_instance_id -> cluster");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("cluster", "opennms-us-east");
        assertThat(out.labels()).doesNotContainKey("onms_instance_id");
    }

    @Test
    void onms_instance_id_is_emitted_first_in_iteration_order() {
        // Design decision §3 pins emission position: onms_instance_id goes in
        // first. Iteration order of the emitted Map is load-bearing for the
        // exclude/include/rename passes downstream, so guard against a future
        // refactor that silently moves the put() call.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setInstanceId("opennms-us-east");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        List<String> keys = new ArrayList<>(out.labels().keySet());
        assertThat(keys.get(0)).isEqualTo("onms_instance_id");
        assertThat(keys.get(1)).isEqualTo("__name__");
    }

    @Test
    void onms_instance_id_value_is_sanitized_to_label_value_rules() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        // Sanitizer.labelValue truncates values exceeding 2048 bytes;
        // we just assert the label is emitted with the literal value
        // for typical operator-supplied identifiers.
        c.setInstanceId("opennms-us-east-1");
        MappedSample out = new LabelMapper(c).map(interfaceSample());
        assertThat(out.labels()).containsEntry("onms_instance_id", "opennms-us-east-1");
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

    // ---------- consumed-keys dedup (v0.2) ----------------------------------

    @Test
    void labels_include_star_does_not_re_emit_consumed_source_keys() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("*");
        MappedSample out = new LabelMapper(c).map(fullFixtureSample());
        // Five duplicates v0.1 produced under `labels.include = *` are gone:
        // the source-key snake-cased forms that did NOT collide with a default
        // label name, and therefore slipped past v0.1's putIfAbsent dedup.
        assertThat(out.labels()).doesNotContainKey("name");
        assertThat(out.labels()).doesNotContainKey("resource_id");
        assertThat(out.labels()).doesNotContainKey("if_high_speed");
        assertThat(out.labels()).doesNotContainKey("node_id");
        assertThat(out.labels()).doesNotContainKey("categories");
    }

    @Test
    void labels_include_star_preserves_default_allowlist_labels() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("*");
        MappedSample out = new LabelMapper(c).map(fullFixtureSample());
        assertThat(out.labels()).containsEntry("__name__", "ifHCInOctets");
        assertThat(out.labels()).containsKey("resourceId");
        assertThat(out.labels()).containsKey("node");
        assertThat(out.labels()).containsEntry("foreign_source", "NOC");
        assertThat(out.labels()).containsEntry("foreign_id", "router-42");
        assertThat(out.labels()).containsEntry("node_label", "router-42.example.com");
        assertThat(out.labels()).containsEntry("location", "default");
        assertThat(out.labels()).containsEntry("if_name", "eth0");
        assertThat(out.labels()).containsEntry("if_descr", "GigabitEthernet0/0");
        assertThat(out.labels()).containsEntry("if_speed", "1000000000");
        assertThat(out.labels()).containsEntry("onms_cat_Routers", "true");
        assertThat(out.labels()).containsEntry("onms_cat_ProductionSites", "true");
    }

    @Test
    void labels_include_star_does_not_introduce_collide_on_name_duplicates() {
        // Regression: v0.1's putIfAbsent already blocked these 7 labels from
        // being duplicated under `labels.include = *` because their
        // snake-cased source-key forms collide with a default label name.
        // The consumed-keys mechanism must preserve that single-value property.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("*");
        MappedSample out = new LabelMapper(c).map(fullFixtureSample());
        assertThat(out.labels().get("foreign_source")).isEqualTo("NOC");
        assertThat(out.labels().get("foreign_id")).isEqualTo("router-42");
        assertThat(out.labels().get("node_label")).isEqualTo("router-42.example.com");
        assertThat(out.labels().get("location")).isEqualTo("default");
        assertThat(out.labels().get("if_name")).isEqualTo("eth0");
        assertThat(out.labels().get("if_descr")).isEqualTo("GigabitEthernet0/0");
        assertThat(out.labels().get("if_speed")).isEqualTo("1000000000");
    }

    @Test
    void labels_include_narrow_globs_surface_non_default_source_tags() {
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsInclude("sys*, asset*");
        ImmutableMetric.MetricBuilder mb = fullFixtureBuilder()
                .externalTag("sysDescription", "Linux 5.15")
                .externalTag("assetRegion", "us-east");
        MappedSample out = new LabelMapper(c).map(sample(mb));
        assertThat(out.labels()).containsEntry("sys_description", "Linux 5.15");
        assertThat(out.labels()).containsEntry("asset_region", "us-east");
    }

    @Test
    void rename_of_default_plus_include_star_does_not_re_emit_source_key() {
        // In v0.1 this pair produced both `foreign_source_raw` (renamed
        // default) and `foreign_source` (re-surfaced from the `foreignSource`
        // source tag via labels.include = *). Under consumed-keys the source
        // key is skipped, so only `foreign_source_raw` remains.
        PrometheusRemoteWriterConfig c = defaultConfig();
        c.setLabelsRename("foreign_source -> foreign_source_raw");
        c.setLabelsInclude("*");
        MappedSample out = new LabelMapper(c).map(fullFixtureSample());
        assertThat(out.labels()).containsEntry("foreign_source_raw", "NOC");
        assertThat(out.labels()).doesNotContainKey("foreign_source");
    }

    // ---------- drift guard -------------------------------------------------

    @Test
    void consumed_keys_covers_all_buildDefaults_source_reads() {
        // Given a fixture carrying every source key buildDefaults currently
        // reads, the returned consumedSourceKeys set must equal that set
        // exactly. Reviewers adding a new source-key read to buildDefaults
        // must update this test — that's the point.
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("name", "ifHCInOctets");
        tags.put("resourceId", "nodeSource[NOC:router-42].interfaceSnmp[eth0]");
        tags.put("foreignSource", "NOC");
        tags.put("foreignId", "router-42");
        tags.put("nodeLabel", "router-42.example.com");
        tags.put("location", "default");
        tags.put("ifName", "eth0");
        tags.put("ifDescr", "GigabitEthernet0/0");
        tags.put("ifHighSpeed", "1000");
        tags.put("ifSpeed", "4294967295");
        tags.put("nodeId", "42");
        tags.put("categories", "Routers, ProductionSites");

        LabelMapper.Defaults defaults = LabelMapper.buildDefaults("ifHCInOctets", tags, null);

        assertThat(defaults.consumedSourceKeys()).containsExactlyInAnyOrder(
                "name",
                "resourceId",
                "foreignSource",
                "foreignId",
                "nodeLabel",
                "location",
                "ifName",
                "ifDescr",
                "ifHighSpeed",
                "ifSpeed",
                "nodeId",
                "categories");
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

    /** Fixture carrying every source key buildDefaults consults, including
     *  `nodeId` (redundant with FS-qualified identity but exercised by the
     *  consumed-keys dedup tests). */
    private static Sample fullFixtureSample() {
        return sample(fullFixtureBuilder());
    }

    private static ImmutableMetric.MetricBuilder fullFixtureBuilder() {
        return ImmutableMetric.builder()
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
                .externalTag("nodeId", "42")
                .externalTag("categories", "Routers, ProductionSites");
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
