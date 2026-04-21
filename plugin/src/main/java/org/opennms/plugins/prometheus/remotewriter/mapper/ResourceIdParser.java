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
 * Parses OpenNMS hierarchical {@code resourceId} strings like
 * {@code node[1].interfaceSnmp[en0]} or
 * {@code nodeSource[myFS:myFID].hrStorageIndex[1]} into their structured
 * components. A parse failure returns {@code null} — callers emit only the
 * raw {@code resourceId} label in that case.
 */
public final class ResourceIdParser {

    /**
     * Matches {@code (node|nodeSource)[nodeId].resourceType[resourceInstance]}.
     * The instance group is greedy to the terminating {@code ]} and may be empty.
     */
    private static final Pattern PATTERN = Pattern.compile(
            "(?:node|nodeSource)\\[([^\\]]+)\\]\\.([^\\[]+)\\[([^\\]]*)\\]");

    public record Parsed(String nodeId, String resourceType, String resourceInstance) {}

    private ResourceIdParser() {}

    /** Returns the parsed components, or {@code null} on no-match. */
    public static Parsed tryParse(String resourceId) {
        if (resourceId == null || resourceId.isEmpty()) return null;
        Matcher m = PATTERN.matcher(resourceId);
        if (!m.matches()) return null;
        return new Parsed(m.group(1), m.group(2), m.group(3));
    }
}
