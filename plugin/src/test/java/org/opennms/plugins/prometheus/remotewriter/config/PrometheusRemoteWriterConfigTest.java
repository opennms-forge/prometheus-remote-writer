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

    // ---------- instance.id -------------------------------------------------

    @Test
    void instance_id_is_parsed_and_normalised() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("opennms-us-east");
        assertThat(c.getInstanceId()).isEqualTo("opennms-us-east");
    }

    @Test
    void instance_id_whitespace_normalises_to_null() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("   ");
        assertThat(c.getInstanceId()).isNull();
        c.setInstanceId("");
        assertThat(c.getInstanceId()).isNull();
        c.setInstanceId(null);
        assertThat(c.getInstanceId()).isNull();
    }

    @Test
    void instance_id_unicode_whitespace_normalises_to_null() {
        // String.trim() only strips <= U+0020; strip() is Unicode-aware and
        // catches NBSP / ideographic space / other Unicode whitespace. Without
        // this, invisible-character-only values would pass through as valid.
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("\u00a0");          // NBSP
        assertThat(c.getInstanceId()).isNull();
        c.setInstanceId("\u3000");          // ideographic space
        assertThat(c.getInstanceId()).isNull();
        c.setInstanceId("\u00a0 \u3000 ");  // mixed whitespace
        assertThat(c.getInstanceId()).isNull();
    }

    @Test
    void instance_id_with_control_characters_is_rejected_at_validate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("foo\nbar");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("instance.id")
            .hasMessageContaining("control characters");
    }

    @Test
    void instance_id_with_null_byte_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("foo\0bar");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("control characters");
    }

    @Test
    void instance_id_longer_than_2048_bytes_is_rejected_at_validate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("a".repeat(2049));
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("instance.id")
            .hasMessageContaining("2048");
    }

    @Test
    void instance_id_at_2048_bytes_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("a".repeat(2048));
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void instance_id_is_trimmed() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("  opennms-us-east  ");
        assertThat(c.getInstanceId()).isEqualTo("opennms-us-east");
    }

    @Test
    void instance_id_is_reported_in_diff() {
        PrometheusRemoteWriterConfig before = minimal();
        PrometheusRemoteWriterConfig after  = minimal();
        after.setInstanceId("opennms-us-east");
        assertThat(after.diff(before))
            .anyMatch(l -> l.startsWith("instance.id: (unset) -> \"opennms-us-east\""));
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

    // ---------- labels.rename reserved-target validation --------------------

    @Test
    void labels_rename_target_matching_default_label_name_is_rejected() {
        // Parameterised-by-hand: every exact reserved name must be rejected.
        String[] reserved = {
            "__name__", "resourceId", "node",
            "foreign_source", "foreign_id", "node_label", "location",
            "resource_type", "resource_instance",
            "if_name", "if_descr", "if_speed",
            "onms_instance_id"
        };
        for (String name : reserved) {
            PrometheusRemoteWriterConfig c = minimal();
            c.setLabelsRename("foo -> " + name);
            assertThatThrownBy(c::validate)
                .as("rename target '%s' should be rejected", name)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("labels.rename")
                .hasMessageContaining("'" + name + "'");
        }
    }

    @Test
    void labels_rename_target_matching_onms_cat_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> onms_cat_Router");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("onms_cat_")
            .hasMessageContaining("surveillance categories");
    }

    @Test
    void labels_rename_target_matching_onms_meta_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> onms_meta_custom");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("onms_meta_")
            .hasMessageContaining("metadata passthrough");
    }

    @Test
    void labels_rename_two_entries_with_same_target_are_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foreign_source -> cluster, location -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("same target")
            .hasMessageContaining("'cluster'");
    }

    @Test
    void labels_rename_target_onms_instance_id_is_rejected_even_when_instance_id_is_unset() {
        // Reserved-unconditionally guarantee: the operator cannot set a
        // latent-unsafe rename now and have it silently break after a
        // hot-reload enables instance.id.
        PrometheusRemoteWriterConfig c = minimal();
        assertThat(c.getInstanceId()).isNull();
        c.setLabelsRename("node -> onms_instance_id");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("onms_instance_id");
    }

    @Test
    void labels_rename_to_non_reserved_name_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foreign_source -> hostname, location -> region");
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void labels_rename_multiple_errors_are_accumulated_into_one_exception() {
        // foo -> __name__     -> reserved exact
        // bar -> onms_cat_x   -> reserved prefix
        // baz -> cluster      -> ok (first time)
        // qux -> cluster      -> duplicate target of baz
        // Expect 3 distinct errors reported in one IllegalStateException.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> __name__, bar -> onms_cat_x, baz -> cluster, qux -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename has 3 errors")
            .hasMessageContaining("__name__")
            .hasMessageContaining("onms_cat_")
            .hasMessageContaining("same target");
    }

    @Test
    void labels_rename_single_error_uses_singular_phrasing() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> __name__");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename has 1 error:")
            .hasMessageContaining("__name__");
    }

    @Test
    void labels_rename_duplicate_from_entries_are_rejected_at_parse() {
        // 'a -> cluster, a -> tenant' parses to {a=tenant} under a plain
        // LinkedHashMap put; silently dropping the first entry is a
        // copy-paste-error footgun. Reject at labelsRenameMap() — surfaces
        // during validate() too since validate() calls labelsRenameMap().
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a -> cluster, a -> tenant");
        assertThatThrownBy(c::labelsRenameMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("same 'from'")
            .hasMessageContaining("'a'");
    }

    @Test
    void labels_rename_duplicate_from_entries_are_rejected_at_validate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a -> cluster, a -> tenant");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("same 'from'")
            .hasMessageContaining("'a'");
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

    // ---------- labels.copy -------------------------------------------------

    @Test
    void copy_map_is_empty_when_unset() {
        assertThat(minimal().labelsCopyMap()).isEmpty();
    }

    @Test
    void copy_map_parses_single_entry() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> instance");
        assertThat(c.labelsCopyMap()).containsExactly(
            Map.entry("node", List.of("instance")));
    }

    @Test
    void copy_map_parses_multiple_entries_with_whitespace() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node->instance, foreign_source -> tenant ,  ");
        assertThat(c.labelsCopyMap()).containsExactly(
            Map.entry("node", List.of("instance")),
            Map.entry("foreign_source", List.of("tenant")));
    }

    @Test
    void copy_map_preserves_multiple_targets_from_same_source() {
        // Unlike labels.rename, two copies with the same 'from' are allowed:
        // `node -> instance, node -> host` emits both instance and host.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> instance, node -> host");
        assertThat(c.labelsCopyMap()).containsExactly(
            Map.entry("node", List.of("instance", "host")));
    }

    @Test
    void copy_map_rejects_malformed_entry() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("oops");
        assertThatThrownBy(c::labelsCopyMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("from->to");
    }

    @Test
    void copy_map_rejects_empty_sides() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("-> instance");
        assertThatThrownBy(c::labelsCopyMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("empty side");
    }

    @Test
    void copy_to_non_reserved_name_validates() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> instance, foreign_source -> tenant");
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void copy_target_collides_with_default_label_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> foreign_source");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("foreign_source")
            .hasMessageContaining("default label");
    }

    @Test
    void copy_target_with_reserved_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("foo -> onms_cat_router");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("onms_cat_")
            .hasMessageContaining("surveillance categories");
    }

    @Test
    void copy_duplicate_target_across_entries_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> cluster, foreign_source -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("'cluster'")
            .hasMessageContaining("same target");
    }

    @Test
    void copy_target_collides_with_rename_target_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("node -> instance");
        c.setLabelsCopy("foreign_source -> instance");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("'instance'")
            .hasMessageContaining("labels.rename");
    }

    @Test
    void copy_multiple_errors_are_accumulated_into_one_message() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("a -> node, b -> onms_cat_x, c -> cluster, d -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy has 3 error")
            .hasMessageContaining("'node'")
            .hasMessageContaining("onms_cat_")
            .hasMessageContaining("'cluster'");
    }

    // ---------- helpers -----------------------------------------------------

    private static PrometheusRemoteWriterConfig minimal() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        return c;
    }
}
