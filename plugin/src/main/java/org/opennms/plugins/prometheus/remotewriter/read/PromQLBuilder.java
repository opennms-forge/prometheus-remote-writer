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
import java.util.Comparator;
import java.util.Objects;
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

    /** Build a {@code {…}} selector from a collection of TagMatchers.
     *  Rejects null or empty input: Prometheus rejects an empty selector
     *  with a 400, and silently sending one would mask the real cause. */
    public static String fromMatchers(Collection<TagMatcher> matchers) {
        Objects.requireNonNull(matchers, "matchers");
        if (matchers.isEmpty()) {
            throw new IllegalArgumentException("matchers must not be empty");
        }
        String body = matchers.stream()
                .map(PromQLBuilder::renderMatcher)
                .collect(Collectors.joining(","));
        return "{" + body + "}";
    }

    /**
     * Build a {@code {…}} selector from a metric's intrinsic tags — all
     * matchers are EQUALS. Labels are emitted in lexicographic order by name
     * so the produced selector is stable across runs (matters for log
     * readability and any upstream URL caching).
     */
    public static String fromIntrinsicTags(Set<Tag> intrinsicTags) {
        Objects.requireNonNull(intrinsicTags, "intrinsicTags");
        if (intrinsicTags.isEmpty()) {
            throw new IllegalArgumentException("intrinsic tags must not be empty");
        }
        String body = intrinsicTags.stream()
                .map(t -> new String[] { renderLabel(t.getKey()), escape(t.getValue()) })
                .sorted(Comparator.comparing(pair -> pair[0]))
                .map(pair -> pair[0] + "=\"" + pair[1] + "\"")
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

    /** Escape PromQL label-value grammar: backslash, double-quote, and all
     *  C0 control characters (newline, carriage return, tab, etc.) so values
     *  cannot produce log-line injection or break selector parsing on strict
     *  Prometheus forks. */
    static String escape(String v) {
        if (v == null || v.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(v.length() + 4);
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"'  -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        // Escape remaining C0 controls using the backslash-u
                        // four-hex-digit form.
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
