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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;

/**
 * Plugin configuration. Blueprint sets each property individually through the
 * ConfigAdmin-backed property placeholder; {@link #validate()} runs once on
 * bean init and throws {@link IllegalStateException} with an actionable
 * message if the configuration is not usable. Validation failure causes the
 * downstream {@code TimeSeriesStorage} service bean to fail activation, which
 * leaves the OSGi service unregistered until the operator corrects the file.
 */
public class PrometheusRemoteWriterConfig {

    public enum MetadataCase { PRESERVE, SNAKE_CASE }

    // --- Endpoint ---
    private String writeUrl;
    private String readUrl;

    // --- Source identity ---
    /** Operator-supplied identifier for this OpenNMS instance. When non-empty,
     *  every outbound sample carries an {@code onms_instance_id} label with
     *  this value so operators running multiple OpenNMS instances against a
     *  shared Prometheus-compatible backend can disambiguate samples in
     *  PromQL. Orthogonal to {@link #tenantOrgId}; see README. */
    private String instanceId;

    // --- Auth ---
    private String basicUsername;
    private String basicPassword;
    private String bearerToken;
    private String tenantOrgId;

    // --- TLS ---
    private String tlsCaFile;
    private boolean tlsInsecureSkipVerify;

    // --- Queue & batching ---
    private int  queueCapacity          = 10_000;
    private int  batchSize              = 1_000;
    private long flushIntervalMs        = 1_000L;
    private int  retryMaxAttempts       = 5;
    private long retryInitialBackoffMs  = 250L;
    private long retryMaxBackoffMs      = 10_000L;

    // --- HTTP ---
    private long httpConnectTimeoutMs = 5_000L;
    private long httpReadTimeoutMs    = 30_000L;
    private long httpWriteTimeoutMs   = 30_000L;
    private int  httpMaxConnections   = 16;

    // --- Shutdown ---
    private long shutdownGracePeriodMs = 10_000L;

    // --- Read path ---
    /** How far back findMetrics() looks when no explicit start is provided. */
    private long maxSeriesLookbackSeconds = 7_776_000L; // 90 days

    // --- Label policy ---
    private String labelsInclude;
    private String labelsExclude;
    private String labelsRename;
    private String metricPrefix;

    // --- Metadata passthrough ---
    private boolean      metadataEnabled;
    private String       metadataInclude;
    private String       metadataExclude;
    private String       metadataLabelPrefix = "onms_meta_";
    private MetadataCase metadataCase        = MetadataCase.PRESERVE;

    /**
     * Validate a fully populated config. Called by Blueprint's {@code init-method}
     * through {@link org.opennms.plugins.prometheus.remotewriter.PrometheusRemoteWriterStorage}.
     * @throws IllegalStateException if the configuration is rejected
     */
    public void validate() {
        if (isBlank(writeUrl)) {
            throw new IllegalStateException(
                "write.url is required — configure it in "
                + "etc/org.opennms.plugins.tss.prometheusremotewriter.cfg");
        }
        if (isBlank(readUrl)) {
            throw new IllegalStateException(
                "read.url is required — configure it in "
                + "etc/org.opennms.plugins.tss.prometheusremotewriter.cfg");
        }
        if (hasBasicAuth() && hasBearerAuth()) {
            throw new IllegalStateException(
                "auth.basic.* and auth.bearer.token are mutually exclusive — "
                + "configure one or neither, not both");
        }
        if (hasBasicAuth() && (isBlank(basicUsername) || isBlank(basicPassword))) {
            throw new IllegalStateException(
                "Basic auth requires both auth.basic.username and auth.basic.password");
        }
        if (queueCapacity < 1) {
            throw new IllegalStateException("queue.capacity must be >= 1");
        }
        if (batchSize < 1) {
            throw new IllegalStateException("batch.size must be >= 1");
        }
        if (batchSize > queueCapacity) {
            throw new IllegalStateException(
                "batch.size (" + batchSize + ") must not exceed queue.capacity ("
                + queueCapacity + ")");
        }
        if (flushIntervalMs < 1) {
            throw new IllegalStateException("flush.interval-ms must be >= 1");
        }
        if (retryMaxAttempts < 0) {
            throw new IllegalStateException("retry.max-attempts must be >= 0");
        }
        if (retryInitialBackoffMs < 0 || retryMaxBackoffMs < retryInitialBackoffMs) {
            throw new IllegalStateException(
                "retry.initial-backoff-ms must be >= 0 and <= retry.max-backoff-ms");
        }
        if (httpMaxConnections < 1) {
            throw new IllegalStateException("http.max-connections must be >= 1");
        }
        if (shutdownGracePeriodMs < 0) {
            throw new IllegalStateException("shutdown.grace-period-ms must be >= 0");
        }
    }

    public boolean hasBasicAuth()  { return !isBlank(basicUsername) || !isBlank(basicPassword); }
    public boolean hasBearerAuth() { return !isBlank(bearerToken); }
    public boolean hasTenant()     { return !isBlank(tenantOrgId); }

    public List<String> labelsIncludeGlobs() { return parseCsv(labelsInclude); }
    public List<String> labelsExcludeGlobs() { return parseCsv(labelsExclude); }
    public List<String> metadataIncludeGlobs() { return parseCsv(metadataInclude); }
    public List<String> metadataExcludeGlobs() { return parseCsv(metadataExclude); }

    /** Renames parsed as a { from -> to } map; order preserved. */
    public Map<String, String> labelsRenameMap() {
        if (isBlank(labelsRename)) {
            return Collections.emptyMap();
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (String pair : labelsRename.split(",")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                // Tolerate trailing/internal empty CSV segments (e.g. "a->b,,c->d"
                // or "a->b, ").
                continue;
            }
            String[] parts = trimmed.split("->");
            if (parts.length != 2) {
                throw new IllegalStateException(
                    "labels.rename entry must be 'from->to', got: " + pair);
            }
            String from = parts[0].trim();
            String to   = parts[1].trim();
            if (from.isEmpty() || to.isEmpty()) {
                throw new IllegalStateException(
                    "labels.rename entry has empty side: " + pair);
            }
            out.put(from, to);
        }
        return Collections.unmodifiableMap(out);
    }

    /**
     * Human-readable diff against another config, suitable for logging on
     * hot-reload. Returns an empty list when the two configs are equal.
     * Secrets (passwords, bearer token) are masked.
     */
    public List<String> diff(PrometheusRemoteWriterConfig other) {
        if (other == null) {
            return Collections.singletonList("(no prior config)");
        }
        List<String> out = new ArrayList<>();
        diffStr(out, "write.url",                 other.writeUrl,              writeUrl);
        diffStr(out, "read.url",                  other.readUrl,               readUrl);
        diffStr(out, "instance.id",               other.instanceId,            instanceId);
        diffStr(out, "auth.basic.username",       other.basicUsername,         basicUsername);
        diffMasked(out, "auth.basic.password",    other.basicPassword,         basicPassword);
        diffMasked(out, "auth.bearer.token",      other.bearerToken,           bearerToken);
        diffStr(out, "tenant.org-id",             other.tenantOrgId,           tenantOrgId);
        diffStr(out, "tls.ca-file",               other.tlsCaFile,             tlsCaFile);
        diffBool(out, "tls.insecure-skip-verify", other.tlsInsecureSkipVerify, tlsInsecureSkipVerify);
        diffInt(out, "queue.capacity",            other.queueCapacity,         queueCapacity);
        diffInt(out, "batch.size",                other.batchSize,             batchSize);
        diffLong(out, "flush.interval-ms",        other.flushIntervalMs,       flushIntervalMs);
        diffInt(out, "retry.max-attempts",        other.retryMaxAttempts,      retryMaxAttempts);
        diffLong(out, "retry.initial-backoff-ms", other.retryInitialBackoffMs, retryInitialBackoffMs);
        diffLong(out, "retry.max-backoff-ms",     other.retryMaxBackoffMs,     retryMaxBackoffMs);
        diffLong(out, "http.connect-timeout-ms",  other.httpConnectTimeoutMs,  httpConnectTimeoutMs);
        diffLong(out, "http.read-timeout-ms",     other.httpReadTimeoutMs,     httpReadTimeoutMs);
        diffLong(out, "http.write-timeout-ms",    other.httpWriteTimeoutMs,    httpWriteTimeoutMs);
        diffInt(out, "http.max-connections",      other.httpMaxConnections,    httpMaxConnections);
        diffLong(out, "shutdown.grace-period-ms", other.shutdownGracePeriodMs, shutdownGracePeriodMs);
        diffLong(out, "max-series-lookback-seconds", other.maxSeriesLookbackSeconds, maxSeriesLookbackSeconds);
        diffStr(out, "labels.include",            other.labelsInclude,         labelsInclude);
        diffStr(out, "labels.exclude",            other.labelsExclude,         labelsExclude);
        diffStr(out, "labels.rename",             other.labelsRename,          labelsRename);
        diffStr(out, "metric.prefix",             other.metricPrefix,          metricPrefix);
        diffBool(out, "metadata.enabled",         other.metadataEnabled,       metadataEnabled);
        diffStr(out, "metadata.include",          other.metadataInclude,       metadataInclude);
        diffStr(out, "metadata.exclude",          other.metadataExclude,       metadataExclude);
        diffStr(out, "metadata.label-prefix",     other.metadataLabelPrefix,   metadataLabelPrefix);
        diffStr(out, "metadata.case",             other.metadataCase.name(),   metadataCase.name());
        return out;
    }

    // ---------- Setters (Blueprint property binding) --------------------------

    public void setWriteUrl(String v)              { writeUrl = blankToNull(v); }
    public void setReadUrl(String v)               { readUrl = blankToNull(v); }
    public void setInstanceId(String v)            { instanceId = blankToNull(v); }
    public void setBasicUsername(String v)         { basicUsername = blankToNull(v); }
    public void setBasicPassword(String v)         { basicPassword = blankToNull(v); }
    public void setBearerToken(String v)           { bearerToken = blankToNull(v); }
    public void setTenantOrgId(String v)           { tenantOrgId = blankToNull(v); }
    public void setTlsCaFile(String v)             { tlsCaFile = blankToNull(v); }
    public void setTlsInsecureSkipVerify(boolean v){ tlsInsecureSkipVerify = v; }
    public void setQueueCapacity(int v)            { queueCapacity = v; }
    public void setBatchSize(int v)                { batchSize = v; }
    public void setFlushIntervalMs(long v)         { flushIntervalMs = v; }
    public void setRetryMaxAttempts(int v)         { retryMaxAttempts = v; }
    public void setRetryInitialBackoffMs(long v)   { retryInitialBackoffMs = v; }
    public void setRetryMaxBackoffMs(long v)       { retryMaxBackoffMs = v; }
    public void setHttpConnectTimeoutMs(long v)    { httpConnectTimeoutMs = v; }
    public void setHttpReadTimeoutMs(long v)       { httpReadTimeoutMs = v; }
    public void setHttpWriteTimeoutMs(long v)      { httpWriteTimeoutMs = v; }
    public void setHttpMaxConnections(int v)       { httpMaxConnections = v; }
    public void setShutdownGracePeriodMs(long v)      { shutdownGracePeriodMs = v; }
    public void setMaxSeriesLookbackSeconds(long v)   { maxSeriesLookbackSeconds = v; }
    public void setLabelsInclude(String v)         { labelsInclude = blankToNull(v); }
    public void setLabelsExclude(String v)         { labelsExclude = blankToNull(v); }
    public void setLabelsRename(String v)          { labelsRename = blankToNull(v); }
    public void setMetricPrefix(String v)          { metricPrefix = blankToNull(v); }
    public void setMetadataEnabled(boolean v)      { metadataEnabled = v; }
    public void setMetadataInclude(String v)       { metadataInclude = blankToNull(v); }
    public void setMetadataExclude(String v)       { metadataExclude = blankToNull(v); }
    public void setMetadataLabelPrefix(String v) {
        String trimmed = blankToNull(v);
        // Sanitize the prefix to the Prometheus label-name grammar so an
        // operator-supplied "my-prefix." can't produce an invalid label name
        // downstream. An empty prefix is allowed (no namespacing).
        metadataLabelPrefix = trimmed == null ? "onms_meta_" : Sanitizer.labelName(trimmed);
    }
    public void setMetadataCase(String v) {
        if (isBlank(v)) {
            metadataCase = MetadataCase.PRESERVE;
            return;
        }
        String normalized = v.trim().toUpperCase().replace('-', '_');
        try {
            metadataCase = MetadataCase.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "metadata.case must be 'preserve' or 'snake_case', got: " + v);
        }
    }
    // Aries Blueprint requires at least one setter to match the getter's
    // return type (JavaBean property contract). Without this overload the
    // String setter above is the only pairing candidate and blueprint
    // rejects the bean with "At least one Setter method has to match the
    // type of the Getter method for property metadataCase".
    public void setMetadataCase(MetadataCase v) {
        metadataCase = v == null ? MetadataCase.PRESERVE : v;
    }

    // ---------- Getters -------------------------------------------------------

    public String  getWriteUrl()              { return writeUrl; }
    public String  getReadUrl()               { return readUrl; }
    public String  getInstanceId()            { return instanceId; }
    public String  getBasicUsername()         { return basicUsername; }
    public String  getBasicPassword()         { return basicPassword; }
    public String  getBearerToken()           { return bearerToken; }
    public String  getTenantOrgId()           { return tenantOrgId; }
    public String  getTlsCaFile()             { return tlsCaFile; }
    public boolean isTlsInsecureSkipVerify()  { return tlsInsecureSkipVerify; }
    public int     getQueueCapacity()         { return queueCapacity; }
    public int     getBatchSize()             { return batchSize; }
    public long    getFlushIntervalMs()       { return flushIntervalMs; }
    public int     getRetryMaxAttempts()      { return retryMaxAttempts; }
    public long    getRetryInitialBackoffMs() { return retryInitialBackoffMs; }
    public long    getRetryMaxBackoffMs()     { return retryMaxBackoffMs; }
    public long    getHttpConnectTimeoutMs()  { return httpConnectTimeoutMs; }
    public long    getHttpReadTimeoutMs()     { return httpReadTimeoutMs; }
    public long    getHttpWriteTimeoutMs()    { return httpWriteTimeoutMs; }
    public int     getHttpMaxConnections()    { return httpMaxConnections; }
    public long    getShutdownGracePeriodMs()    { return shutdownGracePeriodMs; }
    public long    getMaxSeriesLookbackSeconds() { return maxSeriesLookbackSeconds; }
    public String  getLabelsInclude()         { return labelsInclude; }
    public String  getLabelsExclude()         { return labelsExclude; }
    public String  getLabelsRename()          { return labelsRename; }
    public String  getMetricPrefix()          { return metricPrefix; }
    public boolean isMetadataEnabled()        { return metadataEnabled; }
    public String  getMetadataInclude()       { return metadataInclude; }
    public String  getMetadataExclude()       { return metadataExclude; }
    public String  getMetadataLabelPrefix()   { return metadataLabelPrefix; }
    public MetadataCase getMetadataCase()     { return metadataCase; }

    // ---------- helpers -------------------------------------------------------

    private static boolean isBlank(String s) { return s == null || s.isEmpty(); }

    private static String blankToNull(String s) {
        if (s == null) return null;
        String trimmed = s.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static List<String> parseCsv(String csv) {
        if (isBlank(csv)) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(
            Arrays.stream(csv.split(","))
                  .map(String::trim)
                  .filter(s -> !s.isEmpty())
                  .toList());
    }

    private static void diffStr(List<String> out, String key, String before, String after) {
        if (!Objects.equals(before, after)) {
            out.add(key + ": " + show(before) + " -> " + show(after));
        }
    }

    private static void diffMasked(List<String> out, String key, String before, String after) {
        if (!Objects.equals(before, after)) {
            out.add(key + ": " + mask(before) + " -> " + mask(after));
        }
    }

    private static void diffBool(List<String> out, String key, boolean before, boolean after) {
        if (before != after) {
            out.add(key + ": " + before + " -> " + after);
        }
    }

    private static void diffInt(List<String> out, String key, int before, int after) {
        if (before != after) {
            out.add(key + ": " + before + " -> " + after);
        }
    }

    private static void diffLong(List<String> out, String key, long before, long after) {
        if (before != after) {
            out.add(key + ": " + before + " -> " + after);
        }
    }

    private static String show(String s)  { return s == null ? "(unset)" : "\"" + s + "\""; }
    private static String mask(String s)  { return s == null ? "(unset)" : "(set)"; }
}
