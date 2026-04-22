/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.sanitize;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class SanitizerTest {

    // ---------- metric name -------------------------------------------------

    @Test
    void metric_name_preserves_valid_characters() {
        assertThat(Sanitizer.metricName("ifHCInOctets")).isEqualTo("ifHCInOctets");
        assertThat(Sanitizer.metricName("http:requests:total")).isEqualTo("http:requests:total");
        assertThat(Sanitizer.metricName("node_cpu_usage_1")).isEqualTo("node_cpu_usage_1");
    }

    @Test
    void metric_name_replaces_forbidden_chars() {
        assertThat(Sanitizer.metricName("ifHC-In-Octets")).isEqualTo("ifHC_In_Octets");
        assertThat(Sanitizer.metricName("foo.bar")).isEqualTo("foo_bar");
        assertThat(Sanitizer.metricName("weird char!")).isEqualTo("weird_char_");
    }

    @Test
    void metric_name_leading_digit_replaced() {
        assertThat(Sanitizer.metricName("1minuteLoad")).isEqualTo("_minuteLoad");
    }

    @Test
    void metric_name_allows_colon_and_underscore() {
        assertThat(Sanitizer.metricName("_leading_ok")).isEqualTo("_leading_ok");
        assertThat(Sanitizer.metricName(":colon_is_ok")).isEqualTo(":colon_is_ok");
    }

    // ---------- label name --------------------------------------------------

    @Test
    void label_name_preserves_valid_characters() {
        assertThat(Sanitizer.labelName("ifName")).isEqualTo("ifName");
        assertThat(Sanitizer.labelName("node_label")).isEqualTo("node_label");
        assertThat(Sanitizer.labelName("__name__")).isEqualTo("__name__");
    }

    @Test
    void label_name_rejects_colon_unlike_metric_name() {
        // Metric names may contain ':' but label names may not.
        assertThat(Sanitizer.labelName("ns:label")).isEqualTo("ns_label");
    }

    @Test
    void label_name_replaces_forbidden_chars() {
        assertThat(Sanitizer.labelName("my-label")).isEqualTo("my_label");
        assertThat(Sanitizer.labelName("with space")).isEqualTo("with_space");
        assertThat(Sanitizer.labelName("dot.label")).isEqualTo("dot_label");
    }

    @Test
    void label_name_leading_digit_replaced() {
        assertThat(Sanitizer.labelName("2foo")).isEqualTo("_foo");
    }

    // ---------- label value -------------------------------------------------

    @Test
    void label_value_passes_through_short_ascii_strings_unchanged() {
        assertThat(Sanitizer.labelValue("hello world")).isEqualTo("hello world");
        assertThat(Sanitizer.labelValue("")).isEqualTo("");
    }

    @Test
    void label_value_preserves_valid_utf8() {
        // Emoji and multibyte chars pass through when under the byte cap.
        String utf8 = "café — 你好 🐉";
        assertThat(Sanitizer.labelValue(utf8)).isEqualTo(utf8);
    }

    @Test
    void label_value_truncates_long_ascii_to_2048_bytes() {
        String input = "a".repeat(3000);
        String out = Sanitizer.labelValue(input);
        assertThat(out).hasSize(Sanitizer.MAX_LABEL_VALUE_BYTES);
        assertThat(out.getBytes(StandardCharsets.UTF_8).length).isEqualTo(Sanitizer.MAX_LABEL_VALUE_BYTES);
    }

    @Test
    void label_value_does_not_split_a_multibyte_codepoint() {
        // '€' is 3 UTF-8 bytes. Put enough to overflow the cap and force a
        // boundary in the middle of a multi-byte sequence.
        String input = "a".repeat(2047) + "€";  // 2047 + 3 = 2050 bytes
        String out = Sanitizer.labelValue(input);

        int outBytes = out.getBytes(StandardCharsets.UTF_8).length;
        assertThat(outBytes).isLessThanOrEqualTo(Sanitizer.MAX_LABEL_VALUE_BYTES);
        // The '€' must be absent because it couldn't fit cleanly.
        assertThat(out).isEqualTo("a".repeat(2047));
    }

    @Test
    void label_value_handles_null_input() {
        assertThat(Sanitizer.labelValue(null)).isNull();
    }

    @Test
    void label_value_fast_path_returns_same_instance_for_short_strings() {
        // Any string under MAX_LABEL_VALUE_BYTES / 3 chars can't exceed the
        // byte cap and must return unchanged (identity, not just equal).
        String in = "short ascii";
        assertThat(Sanitizer.labelValue(in)).isSameAs(in);

        // Borderline at the fast-path threshold (682 chars × 3 = 2046 ≤ 2048)
        String near = "a".repeat(682);
        assertThat(Sanitizer.labelValue(near)).isSameAs(near);
    }
}
