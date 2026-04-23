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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * Turns an OpenNMS {@link Sample} into a {@link MappedSample} with the
 * opinionated default label set and any operator-configured overrides.
 *
 * <p>Ordering of transformations (fixed):
 * <ol>
 *   <li>Build the default allowlist from the sample's tags</li>
 *   <li>Apply {@code labels.exclude} globs — remove matching label names</li>
 *   <li>Apply {@code labels.include} globs — surface matching source tag keys</li>
 *   <li>Apply {@code labels.rename} — rename label names {@code from -> to}</li>
 * </ol>
 *
 * <p>Label values are sanitized (truncated to {@link Sanitizer#MAX_LABEL_VALUE_BYTES}
 * bytes); label names and the metric name are sanitized to the Prometheus
 * text-model grammar.
 *
 * <p>Source-tag keys this mapper consults:
 * <ul>
 *   <li>{@code name} (intrinsic) — metric name</li>
 *   <li>{@code resourceId} (intrinsic) — kept raw and parsed</li>
 *   <li>{@code nodeId}, {@code foreignSource}, {@code foreignId}, {@code nodeLabel}, {@code location} — node identity</li>
 *   <li>{@code ifName}, {@code ifDescr}, {@code ifSpeed}, {@code ifHighSpeed} — interface attributes</li>
 *   <li>{@code categories} (comma-separated) — surveillance categories</li>
 * </ul>
 * Any other source tag is only surfaced when the operator opts in via
 * {@code labels.include}.
 */
public final class LabelMapper {

    /**
     * Label names emitted unconditionally (modulo config) by {@link #buildDefaults}.
     * Used by {@link PrometheusRemoteWriterConfig#validate} to reject
     * {@code labels.rename} entries whose target would silently clobber a
     * default label at flush time.
     *
     * <p>Keep in sync with {@link #buildDefaults} — a new default-label emission
     * added there must also land here, or operators lose the startup safety net.
     */
    public static final Set<String> RESERVED_LABEL_NAMES = Set.of(
            "__name__",
            "resourceId",
            "node",
            "foreign_source",
            "foreign_id",
            "node_label",
            "location",
            "resource_type",
            "resource_instance",
            "if_name",
            "if_descr",
            "if_speed",
            "onms_instance_id");

    /**
     * Label-name prefixes reserved because multiple labels may be emitted
     * under them. Renaming onto a matching target would collide with one of
     * those emissions at flush time.
     *
     * <p>{@code onms_cat_*} covers per-surveillance-category expansion.
     * {@code onms_meta_*} is the default {@code metadata.label-prefix}; an
     * operator who customizes that prefix is out of scope for this guard.
     * Keep in sync with {@link #buildDefaults} and {@link MetadataProcessor}.
     */
    public static final List<String> RESERVED_LABEL_PREFIXES = List.of(
            "onms_cat_",
            "onms_meta_");

    private final List<Pattern> excludeGlobs;
    private final List<Pattern> includeGlobs;
    private final Map<String, String> renameMap;
    private final String metricPrefix;
    private final String instanceId;
    private final MetadataProcessor metadataProcessor;

    public LabelMapper(PrometheusRemoteWriterConfig config) {
        Objects.requireNonNull(config, "config");
        this.excludeGlobs      = compileGlobs(config.labelsExcludeGlobs());
        this.includeGlobs      = compileGlobs(config.labelsIncludeGlobs());
        this.renameMap         = config.labelsRenameMap();
        this.metricPrefix      = config.getMetricPrefix();
        this.instanceId        = config.getInstanceId();
        this.metadataProcessor = new MetadataProcessor(config);
    }

    public long getMetadataDenylistBlockedCount() {
        return metadataProcessor.getDenylistBlockedCount();
    }

    /**
     * @return a mapped sample, or {@code null} if the input sample has no
     *         metric name (Prometheus requires {@code __name__})
     */
    public MappedSample map(Sample sample) {
        Objects.requireNonNull(sample, "sample");
        Metric metric = sample.getMetric();
        Map<String, String> sourceTags = collectTags(metric);

        String metricName = sourceTags.get(IntrinsicTagNames.name);
        if (metricName == null || metricName.isEmpty()) {
            return null;
        }
        if (metricPrefix != null && !metricPrefix.isEmpty()) {
            metricName = metricPrefix + metricName;
        }

        Defaults defaults = buildDefaults(metricName, sourceTags, instanceId);
        Map<String, String> labels = applyExclude(defaults.labels(), excludeGlobs);
        labels = applyInclude(labels, sourceTags, includeGlobs, defaults.consumedSourceKeys());
        labels = applyRename(labels, renameMap);
        // Metadata passthrough runs last so its prefix-namespaced labels are
        // not renamed or excluded by labels.* rules; the default allowlist
        // still wins on collisions.
        metadataProcessor.emitInto(labels, sourceTags);

        return new MappedSample(
                labels,
                sample.getTime().toEpochMilli(),
                sample.getValue());
    }

    // -- defaults -------------------------------------------------------------

    /**
     * Result of {@link #buildDefaults}: the label map plus the set of source-tag
     * keys the default allowlist consumed. {@link #applyInclude} uses the set to
     * skip keys the defaults already own, preventing {@code labels.include = *}
     * from re-emitting them under a snake-cased alias.
     */
    record Defaults(Map<String, String> labels, Set<String> consumedSourceKeys) {}

    static Defaults buildDefaults(String metricName, Map<String, String> tags, String instanceId) {
        Map<String, String> out = new LinkedHashMap<>();
        Set<String> consumed = new HashSet<>();

        // onms_instance_id — emitted first so it reads as the origin stamp
        // when humans skim the series. Absent when instance.id is unset. The
        // value comes from config, not source tags, so nothing is consumed.
        if (instanceId != null && !instanceId.isEmpty()) {
            out.put("onms_instance_id", Sanitizer.labelValue(instanceId));
        }

        // __name__
        consumed.add(IntrinsicTagNames.name);
        out.put("__name__", Sanitizer.metricName(metricName));

        // resourceId raw + parsed components
        consumed.add(IntrinsicTagNames.resourceId);
        String resourceId = tags.get(IntrinsicTagNames.resourceId);
        ResourceIdParser.Parsed parsed = null;
        if (resourceId != null) {
            out.put("resourceId", Sanitizer.labelValue(resourceId));
            parsed = ResourceIdParser.tryParse(resourceId);
            if (parsed != null) {
                out.put("resource_type",     Sanitizer.labelValue(parsed.resourceType()));
                out.put("resource_instance", Sanitizer.labelValue(parsed.resourceInstance()));
            }
        }

        // node identity — FS-qualified preferred, then parsed nodeId, then numeric dbId.
        // A half-populated pair (one side empty or blank) falls through rather
        // than emitting "fs:" or ":fid" with a dangling colon. All three source
        // keys are marked consumed even on fall-through so `labels.include = *`
        // never re-surfaces them under a snake-cased alias.
        consumed.add("foreignSource");
        consumed.add("foreignId");
        consumed.add("nodeId");
        String fs  = tags.get("foreignSource");
        String fid = tags.get("foreignId");
        if (fs != null && !fs.isEmpty() && fid != null && !fid.isEmpty()) {
            out.put("node", Sanitizer.labelValue(fs + ":" + fid));
        } else if (parsed != null) {
            out.put("node", Sanitizer.labelValue(parsed.nodeId()));
        } else {
            String nodeId = tags.get("nodeId");
            if (nodeId != null && !nodeId.isEmpty()) {
                out.put("node", Sanitizer.labelValue(nodeId));
            }
        }

        consumed.add("nodeLabel");
        consumed.add("location");
        consumed.add("ifName");
        consumed.add("ifDescr");
        putIfPresent(out, "node_label",     tags, "nodeLabel");
        putIfPresent(out, "foreign_source", tags, "foreignSource");
        putIfPresent(out, "foreign_id",     tags, "foreignId");
        putIfPresent(out, "location",       tags, "location");
        putIfPresent(out, "if_name",        tags, "ifName");
        putIfPresent(out, "if_descr",       tags, "ifDescr");

        // if_speed normalisation
        consumed.add("ifHighSpeed");
        consumed.add("ifSpeed");
        Long ifSpeed = IfSpeedNormalizer.normalize(tags.get("ifHighSpeed"), tags.get("ifSpeed"));
        if (ifSpeed != null) {
            out.put("if_speed", Long.toString(ifSpeed));
        }

        // Surveillance categories: one label per category. Accepts a single
        // `categories` tag with comma-separated values — the convention used
        // by OpenNMS's TSS adapter as of v0.1. Revisit in 15.2 if real
        // deployments surface categories differently.
        consumed.add("categories");
        String categories = tags.get("categories");
        if (categories != null && !categories.isEmpty()) {
            for (String cat : categories.split(",")) {
                cat = cat.trim();
                if (!cat.isEmpty()) {
                    String labelName = "onms_cat_" + Sanitizer.labelName(cat);
                    out.put(labelName, "true");
                }
            }
        }
        return new Defaults(out, Set.copyOf(consumed));
    }

    private static void putIfPresent(Map<String, String> out, String labelName,
                                     Map<String, String> source, String sourceKey) {
        String v = source.get(sourceKey);
        if (v != null && !v.isEmpty()) {
            out.put(labelName, Sanitizer.labelValue(v));
        }
    }

    // -- exclude/include/rename ----------------------------------------------

    private static Map<String, String> applyExclude(Map<String, String> labels, List<Pattern> excludeGlobs) {
        if (excludeGlobs.isEmpty()) return labels;
        Map<String, String> out = new LinkedHashMap<>(labels);
        out.keySet().removeIf(name -> matchesAny(name, excludeGlobs));
        return out;
    }

    private static Map<String, String> applyInclude(Map<String, String> labels,
                                                    Map<String, String> sourceTags,
                                                    List<Pattern> includeGlobs,
                                                    Set<String> consumedSourceKeys) {
        if (includeGlobs.isEmpty()) return labels;
        Map<String, String> out = new LinkedHashMap<>(labels);
        for (Map.Entry<String, String> e : sourceTags.entrySet()) {
            // Default allowlist owns these keys — skip so labels.include = *
            // does not re-emit them under a snake-cased alias (e.g. 'name'
            // alongside '__name__', 'resource_id' alongside 'resourceId').
            if (consumedSourceKeys.contains(e.getKey())) continue;
            if (!matchesAny(e.getKey(), includeGlobs)) continue;
            // Match the default allowlist's camelCase → snake_case convention
            // (e.g. ifName → if_name) so an operator writing
            // "labels.include = ifAlias" sees the same shape as the built-ins.
            String labelName = Sanitizer.labelName(MetadataProcessor.toSnakeCase(e.getKey()));
            out.putIfAbsent(labelName, Sanitizer.labelValue(e.getValue()));
        }
        return out;
    }

    private static Map<String, String> applyRename(Map<String, String> labels, Map<String, String> renameMap) {
        if (renameMap.isEmpty()) return labels;
        Map<String, String> out = new LinkedHashMap<>(labels.size());
        for (Map.Entry<String, String> e : labels.entrySet()) {
            String name = e.getKey();
            String renamed = renameMap.get(name);
            if (renamed != null) {
                name = Sanitizer.labelName(renamed);
            }
            out.put(name, e.getValue());
        }
        return out;
    }

    // -- helpers --------------------------------------------------------------

    /** Merge intrinsic + meta + external tags into one map; earlier entries win. */
    private static Map<String, String> collectTags(Metric metric) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Tag t : metric.getIntrinsicTags()) out.putIfAbsent(t.getKey(), t.getValue());
        for (Tag t : metric.getMetaTags())      out.putIfAbsent(t.getKey(), t.getValue());
        for (Tag t : metric.getExternalTags())  out.putIfAbsent(t.getKey(), t.getValue());
        return out;
    }

    private static List<Pattern> compileGlobs(List<String> globs) {
        if (globs.isEmpty()) return List.of();
        List<Pattern> out = new ArrayList<>(globs.size());
        for (String g : globs) {
            out.add(globToPattern(g));
        }
        return List.copyOf(out);
    }

    /**
     * Compile a glob (wildcards {@code *} and {@code ?}) into a case-sensitive regex.
     * Dots, brackets, and other regex metacharacters are escaped.
     */
    static Pattern globToPattern(String glob) {
        return globToPattern(glob, 0);
    }

    /**
     * Compile a glob with {@link Pattern} flags — for denylist patterns that
     * must match regardless of case ({@link Pattern#CASE_INSENSITIVE}).
     */
    static Pattern globToPattern(String glob, int flags) {
        StringBuilder re = new StringBuilder(glob.length() + 4);
        re.append('^');
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*' -> re.append(".*");
                case '?' -> re.append('.');
                case '.', '(', ')', '+', '|', '^', '$', '@', '%', '{', '}', '[', ']', '\\' ->
                    re.append('\\').append(c);
                default  -> re.append(c);
            }
        }
        re.append('$');
        return Pattern.compile(re.toString(), flags);
    }

    private static boolean matchesAny(String s, List<Pattern> patterns) {
        for (Pattern p : patterns) {
            if (p.matcher(s).matches()) return true;
        }
        return false;
    }
}
