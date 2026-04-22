/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PrometheusRemoteWriterConfigTest {

    // ---------- defaults ----------------------------------------------------

    @Test
    void minimal_valid_config_accepts_defaults() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatCode(c::validate).doesNotThrowAnyException();

        assertThat(c.getQueueCapacity()).isEqualTo(10_000);
        assertThat(c.getBatchSize()).isEqualTo(1_000);
        assertThat(c.getFlushIntervalMs()).isEqualTo(1_000L);
        assertThat(c.getRetryMaxAttempts()).isEqualTo(5);
        assertThat(c.getHttpMaxConnections()).isEqualTo(16);
        assertThat(c.getShutdownGracePeriodMs()).isEqualTo(10_000L);
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("onms_meta_");
        assertThat(c.getMetadataCase()).isEqualTo(PrometheusRemoteWriterConfig.MetadataCase.PRESERVE);
        assertThat(c.isMetadataEnabled()).isFalse();
    }

    // ---------- blank/null normalisation ------------------------------------

    @Test
    void blank_string_properties_are_normalized_to_null() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("   ");
        c.setBearerToken("");
        c.setLabelsInclude("   ");
        c.setMetricPrefix("");

        assertThat(c.getBasicUsername()).isNull();
        assertThat(c.getBearerToken()).isNull();
        assertThat(c.getLabelsInclude()).isNull();
        assertThat(c.getMetricPrefix()).isNull();
    }

    // ---------- required endpoints ------------------------------------------

    @Test
    void missing_write_url_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWriteUrl(null);
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("write.url is required");
    }

    @Test
    void missing_read_url_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setReadUrl(null);
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("read.url is required");
    }

    // ---------- auth exclusivity --------------------------------------------

    @Test
    void basic_and_bearer_are_mutually_exclusive() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("u");
        c.setBasicPassword("p");
        c.setBearerToken("tok");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mutually exclusive");
    }

    @Test
    void basic_requires_both_username_and_password() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("u");
        // password intentionally missing
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Basic auth requires both");
    }

    @Test
    void bearer_alone_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBearerToken("tok");
        assertThatCode(c::validate).doesNotThrowAnyException();
        assertThat(c.hasBearerAuth()).isTrue();
        assertThat(c.hasBasicAuth()).isFalse();
    }

    // ---------- numeric bounds ----------------------------------------------

    @Test
    void batch_larger_than_queue_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setQueueCapacity(100);
        c.setBatchSize(500);
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("batch.size")
            .hasMessageContaining("queue.capacity");
    }

    @Test
    void backoff_max_must_be_at_least_initial() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setRetryInitialBackoffMs(1_000);
        c.setRetryMaxBackoffMs(500);
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("retry");
    }

    // ---------- labels.rename parser ----------------------------------------

    @Test
    void rename_map_parses_valid_entries() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo->bar, baz -> qux ,  ");
        Map<String, String> m = c.labelsRenameMap();
        assertThat(m).containsExactly(Map.entry("foo", "bar"), Map.entry("baz", "qux"));
    }

    @Test
    void rename_map_tolerates_internal_and_trailing_empty_entries() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a->b,,c->d,");
        Map<String, String> m = c.labelsRenameMap();
        assertThat(m).containsExactly(Map.entry("a", "b"), Map.entry("c", "d"));
    }

    @Test
    void rename_map_rejects_malformed_entry() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("oops");
        assertThatThrownBy(c::labelsRenameMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("from->to");
    }

    // ---------- glob CSV parser ---------------------------------------------

    @Test
    void include_glob_csv_trims_and_drops_blanks() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsInclude("ifAlias, , sysDescr ");
        assertThat(c.labelsIncludeGlobs()).containsExactly("ifAlias", "sysDescr");
    }

    // ---------- metadata.case parser ----------------------------------------

    @Test
    void metadata_label_prefix_is_sanitized_to_valid_label_name_grammar() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setMetadataLabelPrefix("my-prefix.");
        // Forbidden chars replaced with '_' per Prometheus label-name rules.
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("my_prefix_");
    }

    @Test
    void metadata_label_prefix_default_when_blank() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setMetadataLabelPrefix("   ");
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("onms_meta_");
    }

    @Test
    void metadata_case_accepts_preserve_and_snake_case_spellings() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setMetadataCase("preserve");
        assertThat(c.getMetadataCase()).isEqualTo(PrometheusRemoteWriterConfig.MetadataCase.PRESERVE);
        c.setMetadataCase("snake_case");
        assertThat(c.getMetadataCase()).isEqualTo(PrometheusRemoteWriterConfig.MetadataCase.SNAKE_CASE);
        c.setMetadataCase("snake-case"); // dash variant normalised
        assertThat(c.getMetadataCase()).isEqualTo(PrometheusRemoteWriterConfig.MetadataCase.SNAKE_CASE);
    }

    @Test
    void metadata_case_rejects_unknown_value() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setMetadataCase("camelCase"))
            .isInstanceOf(IllegalStateException.class);
    }

    // ---------- diff --------------------------------------------------------

    @Test
    void diff_against_null_returns_placeholder() {
        List<String> lines = minimal().diff(null);
        assertThat(lines).containsExactly("(no prior config)");
    }

    @Test
    void diff_reports_changed_fields() {
        PrometheusRemoteWriterConfig before = minimal();
        PrometheusRemoteWriterConfig after  = minimal();
        after.setBatchSize(500);
        after.setMetadataEnabled(true);

        List<String> lines = after.diff(before);
        assertThat(lines).hasSize(2);
        assertThat(lines)
            .anyMatch(l -> l.startsWith("batch.size: 1000 -> 500"))
            .anyMatch(l -> l.startsWith("metadata.enabled: false -> true"));
    }

    @Test
    void diff_masks_credentials() {
        PrometheusRemoteWriterConfig before = minimal();
        PrometheusRemoteWriterConfig after  = minimal();
        after.setBasicUsername("u");
        after.setBasicPassword("hunter2");
        after.setBearerToken("dont-leak-me");

        List<String> lines = after.diff(before);
        assertThat(lines).anyMatch(l -> l.contains("auth.basic.password"));
        assertThat(lines).noneMatch(l -> l.contains("hunter2"));
        assertThat(lines).noneMatch(l -> l.contains("dont-leak-me"));
    }

    @Test
    void diff_is_empty_when_configs_match() {
        assertThat(minimal().diff(minimal())).isEmpty();
    }

    // ---------- helpers -----------------------------------------------------

    private static PrometheusRemoteWriterConfig minimal() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        return c;
    }
}
