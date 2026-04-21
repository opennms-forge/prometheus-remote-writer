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

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;

class MetadataProcessorTest {

    // ---------- disabled by default ----------------------------------------

    @Test
    void disabled_by_default_emits_nothing() {
        MetadataProcessor p = new MetadataProcessor(cfg(false, null, null));
        Map<String, String> labels = new LinkedHashMap<>();
        int blocked = p.emitInto(labels, Map.of("requisition:location", "Pittsboro"));
        assertThat(labels).isEmpty();
        assertThat(blocked).isZero();
    }

    // ---------- include match ----------------------------------------------

    @Test
    void include_glob_surfaces_metadata_as_prefixed_label() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "requisition:*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        p.emitInto(labels, Map.of("requisition:location", "Pittsboro"));
        assertThat(labels).containsEntry("onms_meta_requisition_location", "Pittsboro");
    }

    @Test
    void metadata_without_matching_include_is_ignored() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "requisition:*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        p.emitInto(labels, Map.of("node:owner", "NOC"));
        assertThat(labels).isEmpty();
    }

    @Test
    void non_metadata_source_tags_are_ignored() {
        // Keys without ':' are not metadata by convention.
        MetadataProcessor p = new MetadataProcessor(cfg(true, "*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        p.emitInto(labels, Map.of("nodeLabel", "router-42.example.com"));
        assertThat(labels).isEmpty();
    }

    // ---------- built-in denylist ------------------------------------------

    @Test
    void builtin_denylist_blocks_password_keys_even_when_user_allowed() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "requisition:*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        int blocked = p.emitInto(labels, Map.of(
                "requisition:ssh-password", "hunter2",
                "requisition:ssh-user",     "admin"));
        assertThat(labels).containsKey("onms_meta_requisition_ssh_user");
        assertThat(labels).doesNotContainKey("onms_meta_requisition_ssh_password");
        assertThat(labels.values()).doesNotContain("hunter2");
        assertThat(blocked).isEqualTo(1);
    }

    @Test
    void builtin_denylist_blocks_secret_token_key_patterns() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        int blocked = p.emitInto(labels, Map.of(
                "requisition:foo-secret",  "a",
                "requisition:auth-token",  "b",
                "requisition:private-key", "c",
                "node:harmless",           "d"));
        assertThat(labels).containsOnlyKeys("onms_meta_node_harmless");
        assertThat(blocked).isEqualTo(3);
    }

    @Test
    void builtin_denylist_is_case_insensitive() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        int blocked = p.emitInto(labels, Map.of(
                "requisition:PASSWORD", "a",
                "requisition:APIKey",   "b",
                "requisition:Secret",   "c",
                "requisition:MyToken",  "d"));
        assertThat(labels).isEmpty();
        assertThat(blocked).isEqualTo(4);
    }

    @Test
    void builtin_denylist_blocks_literal_snmp_community() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "requisition:*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        int blocked = p.emitInto(labels, Map.of("requisition:snmp-community", "public"));
        assertThat(labels).isEmpty();
        assertThat(blocked).isEqualTo(1);
    }

    @Test
    void user_exclude_applied_after_include() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "node:*", "node:internal-*"));
        Map<String, String> labels = new LinkedHashMap<>();
        p.emitInto(labels, Map.of(
                "node:owner",         "NOC",
                "node:internal-id",   "12345"));
        assertThat(labels).containsOnlyKeys("onms_meta_node_owner");
    }

    // ---------- case conversion --------------------------------------------

    @Test
    void preserve_case_keeps_original_spelling() {
        MetadataProcessor p = new MetadataProcessor(cfgCase(
                PrometheusRemoteWriterConfig.MetadataCase.PRESERVE,
                "node:*"));
        Map<String, String> labels = new LinkedHashMap<>();
        p.emitInto(labels, Map.of("node:rackUnit", "14"));
        assertThat(labels).containsEntry("onms_meta_node_rackUnit", "14");
    }

    @Test
    void snake_case_converts_camel_to_snake() {
        MetadataProcessor p = new MetadataProcessor(cfgCase(
                PrometheusRemoteWriterConfig.MetadataCase.SNAKE_CASE,
                "node:*"));
        Map<String, String> labels = new LinkedHashMap<>();
        p.emitInto(labels, Map.of("node:rackUnit", "14"));
        assertThat(labels).containsEntry("onms_meta_node_rack_unit", "14");
    }

    @Test
    void to_snake_case_handles_pascal_camel_and_uppercase_runs() {
        assertThat(MetadataProcessor.toSnakeCase("RackUnit")).isEqualTo("rack_unit");
        assertThat(MetadataProcessor.toSnakeCase("HTTPServer")).isEqualTo("http_server");
        assertThat(MetadataProcessor.toSnakeCase("APIKey")).isEqualTo("api_key");
        assertThat(MetadataProcessor.toSnakeCase("HTTPProxyURL")).isEqualTo("http_proxy_url");
        assertThat(MetadataProcessor.toSnakeCase("XMLParser")).isEqualTo("xml_parser");
        assertThat(MetadataProcessor.toSnakeCase("httpServer")).isEqualTo("http_server");
        assertThat(MetadataProcessor.toSnakeCase("already_snake")).isEqualTo("already_snake");
        assertThat(MetadataProcessor.toSnakeCase("noChange")).isEqualTo("no_change");
        // Trailing run of uppercase (no lowercase follows) stays together.
        assertThat(MetadataProcessor.toSnakeCase("fooURL")).isEqualTo("foo_url");
        assertThat(MetadataProcessor.toSnakeCase("fooXML")).isEqualTo("foo_xml");
    }

    // ---------- label name sanitation --------------------------------------

    @Test
    void special_characters_in_metadata_keys_are_sanitized() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "custom:*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        p.emitInto(labels, Map.of("custom:rack.unit-42", "yes"));
        assertThat(labels).containsEntry("onms_meta_custom_rack_unit_42", "yes");
    }

    // ---------- stateful counter -------------------------------------------

    @Test
    void denylist_blocked_count_accumulates_across_calls() {
        MetadataProcessor p = new MetadataProcessor(cfg(true, "*", null));
        Map<String, String> labels = new LinkedHashMap<>();
        p.emitInto(labels, Map.of("requisition:password", "a"));
        p.emitInto(labels, Map.of("requisition:secret-value", "b"));
        assertThat(p.getDenylistBlockedCount()).isEqualTo(2);
    }

    // ---------- helpers ----------------------------------------------------

    private static PrometheusRemoteWriterConfig cfg(boolean enabled, String include, String exclude) {
        PrometheusRemoteWriterConfig c = minimal();
        c.setMetadataEnabled(enabled);
        c.setMetadataInclude(include);
        c.setMetadataExclude(exclude);
        return c;
    }

    private static PrometheusRemoteWriterConfig cfgCase(
            PrometheusRemoteWriterConfig.MetadataCase mode, String include) {
        PrometheusRemoteWriterConfig c = minimal();
        c.setMetadataEnabled(true);
        c.setMetadataInclude(include);
        c.setMetadataCase(mode.name().toLowerCase());
        return c;
    }

    private static PrometheusRemoteWriterConfig minimal() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        return c;
    }
}
