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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;

class PromQLBuilderTest {

    @Test
    void translates_name_to_underscore_underscore_name() {
        TagMatcher m = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key(IntrinsicTagNames.name)
                .value("ifHCInOctets")
                .build();
        assertThat(PromQLBuilder.fromMatchers(List.of(m)))
                .isEqualTo("{__name__=\"ifHCInOctets\"}");
    }

    @Test
    void resource_id_is_preserved_as_label_name() {
        TagMatcher m = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key(IntrinsicTagNames.resourceId)
                .value("node[1].interfaceSnmp[eth0]")
                .build();
        assertThat(PromQLBuilder.fromMatchers(List.of(m)))
                .contains("resourceId=\"node[1].interfaceSnmp[eth0]\"");
    }

    @Test
    void supports_all_four_matcher_types() {
        List<TagMatcher> all = List.of(
                ImmutableTagMatcher.builder().type(TagMatcher.Type.EQUALS).key("a").value("x").build(),
                ImmutableTagMatcher.builder().type(TagMatcher.Type.NOT_EQUALS).key("b").value("y").build(),
                ImmutableTagMatcher.builder().type(TagMatcher.Type.EQUALS_REGEX).key("c").value("z.*").build(),
                ImmutableTagMatcher.builder().type(TagMatcher.Type.NOT_EQUALS_REGEX).key("d").value("w.*").build());

        String out = PromQLBuilder.fromMatchers(all);
        assertThat(out).contains("a=\"x\"");
        assertThat(out).contains("b!=\"y\"");
        assertThat(out).contains("c=~\"z.*\"");
        assertThat(out).contains("d!~\"w.*\"");
    }

    @Test
    void escapes_backslash_quote_and_newline_in_values() {
        assertThat(PromQLBuilder.escape("foo\"bar")).isEqualTo("foo\\\"bar");
        assertThat(PromQLBuilder.escape("a\\b")).isEqualTo("a\\\\b");
        assertThat(PromQLBuilder.escape("line1\nline2")).isEqualTo("line1\\nline2");
    }

    @Test
    void sanitizes_unusual_tag_key_to_valid_label_name() {
        TagMatcher m = ImmutableTagMatcher.builder()
                .type(TagMatcher.Type.EQUALS)
                .key("odd-key.1")
                .value("v")
                .build();
        assertThat(PromQLBuilder.fromMatchers(List.of(m)))
                .startsWith("{odd_key_1=\"v\"}");
    }

    @Test
    void builds_selector_from_intrinsic_tags() {
        Set<Tag> tags = Set.of(
                new ImmutableTag(IntrinsicTagNames.name, "ifHCInOctets"),
                new ImmutableTag(IntrinsicTagNames.resourceId, "node[1].interfaceSnmp[eth0]"));
        String out = PromQLBuilder.fromIntrinsicTags(tags);
        assertThat(out).startsWith("{").endsWith("}");
        assertThat(out).contains("__name__=\"ifHCInOctets\"");
        assertThat(out).contains("resourceId=\"node[1].interfaceSnmp[eth0]\"");
    }
}
