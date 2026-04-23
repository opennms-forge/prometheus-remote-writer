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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses OpenNMS hierarchical {@code resourceId} strings into structured
 * components. Three grammars are recognized, tried in order until one matches:
 *
 * <ol>
 *   <li><b>Bracketed</b> — {@code (node|nodeSource)[nodeId].resourceType[resourceInstance]},
 *       e.g. {@code node[1].interfaceSnmp[en0]} or
 *       {@code nodeSource[myFS:myFID].hrStorageIndex[1]}. Emitted by the TSS adapter
 *       for most SNMP collection.</li>
 *   <li><b>Slash-FS</b> — {@code snmp/fs/<foreignSource>/<foreignId>/<group>/<instance…>},
 *       e.g. {@code snmp/fs/selfmonitor/1/opennms-jvm/OpenNMS_Name_Notifd}. Emitted
 *       for self-monitor, JMX, and other filesystem-path collections. The parsed
 *       {@code nodeId} is {@code "fs:fid"} for downstream symmetry with the bracketed
 *       {@code nodeSource[fs:fid]} form.</li>
 *   <li><b>Slash-DB</b> — {@code snmp/<dbNodeId>/<group>/<instance…>},
 *       e.g. {@code snmp/42/hrStorageIndex/1}. Requires a numeric first segment.</li>
 * </ol>
 *
 * <p>The {@code instance} segment in both slash-path forms is greedy — everything
 * after the group segment is captured as {@code resourceInstance}, including any
 * embedded slashes, dots, or colons. JMX MBean object names commonly contain these
 * and we preserve them as a single value rather than truncating.
 *
 * <p>A parse failure for all three grammars returns {@code null} — callers emit only
 * the raw {@code resourceId} label in that case.
 */
public final class ResourceIdParser {

    /**
     * Matches {@code (node|nodeSource)[nodeId].resourceType[resourceInstance]}.
     * The resource-type segment is constrained to an identifier shape
     * ({@code [a-zA-Z][a-zA-Z0-9_-]*}) so degenerate inputs like
     * {@code node[1]..[x]} (which v0.1 accepted with {@code resourceType="."})
     * fall through to raw-only emission. The instance group is greedy to the
     * terminating {@code ]} and may be empty.
     */
    private static final Pattern BRACKETED = Pattern.compile(
            "(?:node|nodeSource)\\[([^\\]]+)\\]\\.([a-zA-Z][a-zA-Z0-9_-]*)\\[([^\\]]*)\\]");

    /**
     * Matches {@code snmp/fs/<foreignSource>/<foreignId>/<group>/<instance…>}.
     * All four path segments are required non-empty and non-whitespace, with
     * brackets rejected to guard against degenerate shapes like
     * {@code snmp/fs/ /1/grp/inst} or {@code snmp/fs/[foo]/1/grp/inst}. Instance
     * is greedy.
     */
    private static final Pattern SLASH_FS = Pattern.compile(
            "snmp/fs/([^/\\s\\[\\]]+)/([^/\\s\\[\\]]+)/([^/\\s\\[\\]]+)/(.+)");

    /**
     * Matches {@code snmp/<dbNodeId>/<group>/<instance…>}. The node id must be
     * a positive integer up to ten digits — disambiguates from non-SNMP paths
     * that begin with {@code snmp/}, rejects {@code 0} (OpenNMS db sequences
     * start at 1) and rejects leading zeros (so {@code 00042} does not produce
     * a {@code node="00042"} label that fails equality against an external
     * {@code nodeId="42"} on the join side). Instance is greedy.
     */
    private static final Pattern SLASH_DB = Pattern.compile(
            "snmp/([1-9]\\d{0,9})/([^/]+)/(.+)");

    public record Parsed(String nodeId, String resourceType, String resourceInstance) {}

    private ResourceIdParser() {}

    /**
     * Returns the parsed components, or {@code null} on no-match.
     *
     * <p>The three grammars are structurally disjoint — a bracketed input
     * never matches a slash pattern and vice versa — so the {@code if}-chain
     * order below is for readability, not correctness. A maintainer reordering
     * the blocks will not change the result set.
     */
    public static Parsed tryParse(String resourceId) {
        if (resourceId == null || resourceId.isEmpty()) return null;

        Matcher m = BRACKETED.matcher(resourceId);
        if (m.matches()) {
            return new Parsed(m.group(1), m.group(2), m.group(3));
        }

        m = SLASH_FS.matcher(resourceId);
        if (m.matches()) {
            return new Parsed(m.group(1) + ":" + m.group(2), m.group(3), m.group(4));
        }

        m = SLASH_DB.matcher(resourceId);
        if (m.matches()) {
            return new Parsed(m.group(1), m.group(2), m.group(3));
        }

        return null;
    }
}
