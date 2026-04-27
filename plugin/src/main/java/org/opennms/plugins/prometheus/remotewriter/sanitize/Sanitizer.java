/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.sanitize;

import java.nio.charset.StandardCharsets;

/**
 * Prometheus text-model sanitization for metric names, label names, and label
 * values.
 *
 * <p>Rules mirror the Apache-2.0 upstream {@code prometheus/common} model code
 * (see {@code model/metric.go} and {@code model/labels.go}). This is a
 * clean-room re-derivation — no code has been copied from the AGPL
 * {@code opennms-cortex-tss-plugin}.
 *
 * <ul>
 *   <li>Metric name: {@code [a-zA-Z_:][a-zA-Z0-9_:]*}</li>
 *   <li>Label name:  {@code [a-zA-Z_][a-zA-Z0-9_]*}</li>
 *   <li>Label value: opaque UTF-8 string, capped at {@value #MAX_LABEL_VALUE_BYTES} bytes</li>
 * </ul>
 */
public final class Sanitizer {

    /** Soft cap on label value byte length. */
    public static final int MAX_LABEL_VALUE_BYTES = 2048;

    private Sanitizer() {}

    /** Sanitize a metric name; forbidden characters become {@code _}, and a
     *  leading digit is replaced with {@code _}. */
    public static String metricName(String in) {
        if (in == null || in.isEmpty()) return in;
        StringBuilder sb = new StringBuilder(in.length());
        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            boolean letter = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
            boolean digit  = c >= '0' && c <= '9';
            boolean ok     = letter || c == '_' || c == ':' || (digit && i > 0);
            sb.append(ok ? c : '_');
        }
        return sb.toString();
    }

    /** Sanitize a label name; forbidden characters become {@code _}, and a
     *  leading digit is replaced with {@code _}. */
    public static String labelName(String in) {
        if (in == null || in.isEmpty()) return in;
        StringBuilder sb = new StringBuilder(in.length());
        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            boolean letter = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
            boolean digit  = c >= '0' && c <= '9';
            boolean ok     = letter || c == '_' || (digit && i > 0);
            sb.append(ok ? c : '_');
        }
        return sb.toString();
    }

    /**
     * Truncate a label value to {@value #MAX_LABEL_VALUE_BYTES} bytes of
     * UTF-8, preserving whole codepoints (never splits a multi-byte sequence).
     * Returns the input unchanged when already under the cap.
     */
    public static String labelValue(String in) {
        if (in == null) return null;
        // Fast path: UTF-8 expands to at most 3 bytes per Java char (BMP chars
        // hit 3 bytes; astral chars use 2 Java chars for 4 UTF-8 bytes — still
        // ≤ 3 bytes/char). If char-count × 3 fits, no encoding work is needed.
        if ((long) in.length() * 3 <= MAX_LABEL_VALUE_BYTES) return in;
        byte[] bytes = in.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_LABEL_VALUE_BYTES) return in;

        int end = MAX_LABEL_VALUE_BYTES;
        // Walk backwards to a codepoint boundary. UTF-8 continuation bytes
        // match 0b10xxxxxx (0x80..0xBF); start bytes don't.
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) {
            end--;
        }
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }
}
