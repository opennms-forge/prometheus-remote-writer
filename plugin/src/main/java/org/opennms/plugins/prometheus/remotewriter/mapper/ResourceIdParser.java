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
     * The instance group is greedy to the terminating {@code ]} and may be empty.
     */
    private static final Pattern BRACKETED = Pattern.compile(
            "(?:node|nodeSource)\\[([^\\]]+)\\]\\.([^\\[]+)\\[([^\\]]*)\\]");

    /**
     * Matches {@code snmp/fs/<foreignSource>/<foreignId>/<group>/<instance…>}.
     * All four path segments are required non-empty; instance is greedy.
     */
    private static final Pattern SLASH_FS = Pattern.compile(
            "snmp/fs/([^/]+)/([^/]+)/([^/]+)/(.+)");

    /**
     * Matches {@code snmp/<dbNodeId>/<group>/<instance…>}. The node id must be
     * numeric to disambiguate from non-SNMP paths that happen to begin with
     * {@code snmp/}; instance is greedy.
     */
    private static final Pattern SLASH_DB = Pattern.compile(
            "snmp/(\\d+)/([^/]+)/(.+)");

    public record Parsed(String nodeId, String resourceType, String resourceInstance) {}

    private ResourceIdParser() {}

    /** Returns the parsed components, or {@code null} on no-match. */
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
