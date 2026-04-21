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

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;

/**
 * Builds PromQL selectors ({@code {name="value",other=~"re"}}) from OpenNMS
 * tag matchers and intrinsic tags. Matches the label schema the plugin uses
 * on the write side: {@code name → __name__}, everything else passes through
 * with label-name sanitization.
 */
public final class PromQLBuilder {

    private PromQLBuilder() {}

    /** Build a {@code {…}} selector from a collection of TagMatchers. */
    public static String fromMatchers(Collection<TagMatcher> matchers) {
        String body = matchers.stream()
                .map(PromQLBuilder::renderMatcher)
                .collect(Collectors.joining(","));
        return "{" + body + "}";
    }

    /**
     * Build a {@code {…}} selector from a metric's intrinsic tags — all
     * matchers are EQUALS.
     */
    public static String fromIntrinsicTags(Set<Tag> intrinsicTags) {
        String body = intrinsicTags.stream()
                .map(t -> renderLabel(t.getKey()) + "=\"" + escape(t.getValue()) + "\"")
                .collect(Collectors.joining(","));
        return "{" + body + "}";
    }

    private static String renderMatcher(TagMatcher m) {
        String label = renderLabel(m.getKey());
        String op = switch (m.getType()) {
            case EQUALS            -> "=";
            case NOT_EQUALS        -> "!=";
            case EQUALS_REGEX      -> "=~";
            case NOT_EQUALS_REGEX  -> "!~";
        };
        return label + op + "\"" + escape(m.getValue()) + "\"";
    }

    private static String renderLabel(String tagKey) {
        if (IntrinsicTagNames.name.equals(tagKey)) {
            return "__name__";
        }
        // IntrinsicTagNames.resourceId is already a valid label name.
        return Sanitizer.labelName(tagKey);
    }

    /** Escape PromQL label-value grammar: backslash, double-quote, newline. */
    static String escape(String v) {
        if (v == null || v.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(v.length() + 4);
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"'  -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                default   -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
