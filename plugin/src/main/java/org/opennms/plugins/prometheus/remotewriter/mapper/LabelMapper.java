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
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * {@code labels.rename} AND {@code labels.copy} entries whose target would
     * silently clobber a default label at flush time.
     *
     * <p>Keep in sync with {@link #buildDefaults} — a new default-label emission
     * added there must also land here, or operators lose the startup safety net.
     *
     * <p>{@code job} and {@code instance} are reserved as of v0.4 now that the
     * plugin emits them as Prom-idiomatic defaults. Operators who previously
     * used {@code labels.rename = foo -> instance} (unusual, since {@code instance}
     * wasn't a default emission pre-v0.4) must pick a different target name.
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
            "onms_instance_id",
            "instance",
            "job");

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

    private static final Logger LOG = LoggerFactory.getLogger(LabelMapper.class);

    private final List<Pattern> excludeGlobs;
    private final List<Pattern> includeGlobs;
    private final Map<String, String> renameMap;
    private final Map<String, List<String>> copyMap;
    private final String metricPrefix;
    private final String instanceId;
    private final String jobName;
    private final MetadataProcessor metadataProcessor;
    /** Plugin metrics sink. May be null — tests that don't care about the
     *  counter use the 1-arg constructor which leaves this null; the
     *  increment in {@link #map(Sample)} is guarded accordingly. */
    private final PluginMetrics metrics;
    /** One-shot WARN gate for unknown labels.copy sources. Entry is added the
     *  first time a source is observed missing (or empty-valued) at copy
     *  time; subsequent occurrences on the same source are silent.
     *  Set-per-instance matches the bundle-lifecycle scope described in the
     *  README. */
    private final Set<String> warnedUnknownCopySources = ConcurrentHashMap.newKeySet();
    /** One-shot WARN gate for labels.copy targets that already exist in the
     *  labels map at copy time (typically because labels.include surfaced a
     *  source tag whose snake-cased name collides with the copy target).
     *  Startup validation catches known collisions against default labels,
     *  reserved prefixes, rename targets, and other copy targets — but
     *  labels.include's source-tag globs are operator-defined and their
     *  resolved snake-cased label names aren't known until a sample flows
     *  through, so runtime detection is the only practical guard. */
    private final Set<String> warnedCopyTargetClobbers = ConcurrentHashMap.newKeySet();

    public LabelMapper(PrometheusRemoteWriterConfig config) {
        this(config, null);
    }

    public LabelMapper(PrometheusRemoteWriterConfig config, PluginMetrics metrics) {
        Objects.requireNonNull(config, "config");
        this.excludeGlobs      = compileGlobs(config.labelsExcludeGlobs());
        this.includeGlobs      = compileGlobs(config.labelsIncludeGlobs());
        this.renameMap         = config.labelsRenameMap();
        this.copyMap           = config.labelsCopyMap();
        this.metricPrefix      = config.getMetricPrefix();
        this.instanceId        = config.getInstanceId();
        this.jobName           = config.getJobName();
        this.metadataProcessor = new MetadataProcessor(config);
        this.metrics           = metrics;
    }

    /** Visible for tests — unmodifiable view of labels.copy sources that
     *  have WARN-emitted via the unknown-source gate on this mapper instance.
     *  Lets tests assert the "exactly one WARN" semantic without depending on
     *  a log-capture framework. */
    Set<String> warnedUnknownCopySourcesForTesting() {
        return java.util.Collections.unmodifiableSet(warnedUnknownCopySources);
    }

    /** Visible for tests — unmodifiable view of labels.copy targets that
     *  have WARN-emitted via the runtime-clobber gate on this mapper
     *  instance. */
    Set<String> warnedCopyTargetClobbersForTesting() {
        return java.util.Collections.unmodifiableSet(warnedCopyTargetClobbers);
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

        Defaults defaults = buildDefaults(metricName, sourceTags, instanceId, jobName);
        if (defaults.resourceIdWasUnparseable() && metrics != null) {
            metrics.samplesUnparseableResourceId(1);
        }
        // Work on a fresh mutable copy; apply{Exclude,Include} may pass the
        // map through unchanged when globs are empty, and metadataProcessor
        // then mutates it — we do not want those mutations to leak back into
        // the Defaults record, which is otherwise treated as a value object.
        Map<String, String> labels = new LinkedHashMap<>(defaults.labels());
        labels = applyExclude(labels, excludeGlobs);
        labels = applyInclude(labels, sourceTags, includeGlobs, defaults.consumedSourceKeys());
        labels = applyCopy(labels, copyMap, warnedUnknownCopySources, warnedCopyTargetClobbers);
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
     * Result of {@link #buildDefaults}: the label map, the set of source-tag
     * keys the default allowlist consumed, and a flag indicating whether the
     * sample's {@code resourceId} failed all three parser grammars (used by
     * {@link #map(Sample)} to increment the
     * {@link PluginMetrics#SAMPLES_UNPARSEABLE_RESOURCE_ID} counter without
     * {@code buildDefaults} itself taking a dependency on {@code PluginMetrics}
     * — keeps the method trivially test-isolable).
     *
     * <p>{@link #applyInclude} uses {@code consumedSourceKeys} to skip keys
     * the defaults already own, preventing {@code labels.include = *} from
     * re-emitting them under a snake-cased alias.
     */
    record Defaults(Map<String, String> labels,
                    Set<String> consumedSourceKeys,
                    boolean resourceIdWasUnparseable) {}

    static Defaults buildDefaults(String metricName, Map<String, String> tags, String instanceId, String jobName) {
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

        // job — collector-type classifier. Emitted unconditionally so
        // `{job=~".+"}` covers every plugin-emitted sample. When job.name is
        // set, it overrides the per-sample derivation with a fleet-wide
        // constant (useful when an operator wants all samples from one plugin
        // instance under the same job value).
        String job = (jobName != null && !jobName.isEmpty())
                ? jobName
                : deriveJob(resourceId, parsed);
        out.put("job", Sanitizer.labelValue(job));

        // node + instance identity — FS-qualified preferred, then parsed
        // nodeId, then numeric dbId. Both labels carry the SAME value; this is
        // the deliberate "instance = subject" stance (see design notes on
        // add-default-job-and-instance-labels for rationale). A half-populated
        // FS pair (one side empty or blank) falls through rather than emitting
        // "fs:" or ":fid" with a dangling colon. All three source keys are
        // marked consumed even on fall-through so `labels.include = *` never
        // re-surfaces them under a snake-cased alias.
        consumed.add("foreignSource");
        consumed.add("foreignId");
        consumed.add("nodeId");
        String nodeValue = null;
        String fs  = tags.get("foreignSource");
        String fid = tags.get("foreignId");
        if (fs != null && !fs.isEmpty() && fid != null && !fid.isEmpty()) {
            nodeValue = fs + ":" + fid;
        } else if (parsed != null) {
            nodeValue = parsed.nodeId();
        } else {
            String nodeId = tags.get("nodeId");
            if (nodeId != null && !nodeId.isEmpty()) {
                nodeValue = nodeId;
            }
        }
        if (nodeValue != null) {
            String sanitized = Sanitizer.labelValue(nodeValue);
            out.put("node",     sanitized);
            out.put("instance", sanitized);
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
        // `parsed == null` captures both "resourceId was null" and "resourceId
        // was present but all three parser grammars missed" — the catch-all
        // branch that also drives `job="opennms"` in deriveJob. Exposed on
        // Defaults so the map() caller can increment the parser-fallthrough
        // counter without passing PluginMetrics into buildDefaults itself.
        boolean resourceIdWasUnparseable = (parsed == null);
        return new Defaults(out, Set.copyOf(consumed), resourceIdWasUnparseable);
    }

    private static void putIfPresent(Map<String, String> out, String labelName,
                                     Map<String, String> source, String sourceKey) {
        String v = source.get(sourceKey);
        if (v != null && !v.isEmpty()) {
            out.put(labelName, Sanitizer.labelValue(v));
        }
    }

    /**
     * Derive the {@code job} label from the sample's {@code resourceId} shape.
     * Mapping (in evaluation order):
     *
     * <ul>
     *   <li>{@code resourceId} starts with {@code "snmp/fs/"} AND the parsed
     *       {@code <group>} segment equals {@code "opennms-jvm"} or starts
     *       with {@code "jmx-"} → {@code "jmx"} (OpenNMS JMX collector
     *       hierarchy: self-monitor, JMX Minion, etc.)</li>
     *   <li>Any other parseable shape (bracketed, slash-DB, other slash-FS
     *       groups) → {@code "snmp"} (the overwhelmingly common case for
     *       OpenNMS data collection)</li>
     *   <li>{@code resourceId} null or unparseable → {@code "opennms"}
     *       (catch-all so dashboards querying {@code {job=~".+"}} cover
     *       every plugin-emitted sample, and operators seeing
     *       {@code job="opennms"} have a visible cue that the parser didn't
     *       recognize the shape)</li>
     * </ul>
     *
     * <p>Overridden by a non-empty {@code job.name} configuration value at
     * the call site — this method is only consulted when the operator hasn't
     * set the override.
     */
    static String deriveJob(String resourceId, ResourceIdParser.Parsed parsed) {
        if (parsed == null || resourceId == null) {
            return "opennms";
        }
        if (resourceId.startsWith("snmp/fs/")) {
            String group = parsed.resourceType();
            // In practice the three ResourceIdParser grammars are structurally
            // disjoint and SLASH_FS's group-3 regex guarantees a non-null
            // non-empty value. The null guard is defensive — cheaper than
            // trusting a future parser refactor not to introduce nulls.
            if (group != null && ("opennms-jvm".equals(group) || group.startsWith("jmx-"))) {
                return "jmx";
            }
        }
        return "snmp";
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

    /**
     * Rename is internally equivalent to a copy-then-drop operation on the
     * same source: the renamed key is emitted under the new name, the old
     * name is removed, and the value is preserved. This equivalence is
     * scoped INSIDE rename — the plugin's {@code labels.exclude} config runs
     * as an earlier pipeline stage (before {@link #applyInclude} and
     * {@link #applyCopy}), so an operator writing
     * {@code labels.copy = node -> cluster, labels.exclude = node} does NOT
     * recreate rename's behavior: the external exclude drops {@code node}
     * before copy can read it. Rename is the only way to get "one label
     * replaces another" semantics at the config surface.
     *
     * <p>We deliberately do NOT express rename as a literal call to
     * {@link #applyCopy} plus an exclude pass: that would shift the renamed
     * key to the tail of the iteration order, whereas walking the input map
     * in place preserves insertion position. Label iteration order is not
     * part of the wire protocol (Prometheus Remote Write sorts labels
     * alphabetically before serialization), but it IS observed by unit tests.
     *
     * <p>If a new rule is added to rename's target validation, mirror it in
     * {@code validateCopyTargets()} — and vice versa.
     */
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

    /**
     * Emit a copy of each listed source label under an additional name. Runs
     * after include, before rename; sees pre-rename label names.
     *
     * <p>One-pass semantics: source lookups read from {@code labels} (the input
     * map is never mutated), so a chain like {@code copy = node -> a, a -> b}
     * does NOT produce {@code b} — at lookup time {@code a} doesn't exist yet
     * in {@code labels}, only in the separate {@code out} map we're building.
     *
     * <p>Empty-valued sources are treated the same as absent sources: skipped
     * with a one-shot WARN. Prometheus treats empty-valued labels as absent at
     * query time, so copying an empty value would produce a label the backend
     * promptly ignores — the operator's intent was almost certainly something
     * else.
     *
     * <p>Targets that already exist in {@code labels} at copy time (typically
     * because {@code labels.include} surfaced a source tag whose snake-cased
     * name happens to equal the copy target) are overwritten, with a one-shot
     * WARN naming the target so operators can spot the silent clobber.
     *
     * <p>Copy targets are trusted to be valid Prometheus label names —
     * {@code PrometheusRemoteWriterConfig.validate()} rejects targets whose
     * sanitized form differs from the raw, so at runtime we can write the
     * target string directly without re-sanitizing.
     */
    private static Map<String, String> applyCopy(Map<String, String> labels,
                                                 Map<String, List<String>> copyMap,
                                                 Set<String> warnedSources,
                                                 Set<String> warnedClobbers) {
        if (copyMap.isEmpty()) return labels;
        Map<String, String> out = new LinkedHashMap<>(labels);
        for (Map.Entry<String, List<String>> e : copyMap.entrySet()) {
            String from = e.getKey();
            String value = labels.get(from);
            if (value == null || value.isEmpty()) {
                if (warnedSources.add(from)) {
                    LOG.warn("labels.copy source '{}' has no value at copy time "
                            + "(absent or empty); directive is a no-op for samples "
                            + "without that label", from);
                }
                continue;
            }
            for (String target : e.getValue()) {
                if (out.containsKey(target) && warnedClobbers.add(target)) {
                    LOG.warn("labels.copy target '{}' already exists in the emitted "
                            + "label set (likely surfaced by labels.include); "
                            + "overwriting. Pick a different target name or narrow "
                            + "the include glob.", target);
                }
                out.put(target, value);
            }
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
