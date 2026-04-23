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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ResourceIdParserTest {

    @Test
    void parses_interface_snmp_resource() {
        ResourceIdParser.Parsed p = ResourceIdParser.tryParse("node[1].interfaceSnmp[en0]");
        assertThat(p).isNotNull();
        assertThat(p.nodeId()).isEqualTo("1");
        assertThat(p.resourceType()).isEqualTo("interfaceSnmp");
        assertThat(p.resourceInstance()).isEqualTo("en0");
    }

    @Test
    void parses_foreign_source_qualified_resource() {
        ResourceIdParser.Parsed p = ResourceIdParser.tryParse("nodeSource[myFS:myFID].hrStorageIndex[1]");
        assertThat(p).isNotNull();
        assertThat(p.nodeId()).isEqualTo("myFS:myFID");
        assertThat(p.resourceType()).isEqualTo("hrStorageIndex");
        assertThat(p.resourceInstance()).isEqualTo("1");
    }

    @Test
    void parses_resource_with_empty_instance() {
        ResourceIdParser.Parsed p = ResourceIdParser.tryParse("node[1].nodeSnmp[]");
        assertThat(p).isNotNull();
        assertThat(p.resourceType()).isEqualTo("nodeSnmp");
        assertThat(p.resourceInstance()).isEqualTo("");
    }

    @Test
    void fails_on_deeply_nested_resource_id() {
        // Not in v0.1 grammar — caller emits raw resourceId only.
        assertThat(ResourceIdParser.tryParse("node[1].http[web1].responseTime")).isNull();
    }

    @Test
    void fails_on_node_only_resource_id() {
        // Node-level resource (no trailing .type[instance]).
        assertThat(ResourceIdParser.tryParse("node[1]")).isNull();
    }

    @Test
    void returns_null_for_blank_input() {
        assertThat(ResourceIdParser.tryParse(null)).isNull();
        assertThat(ResourceIdParser.tryParse("")).isNull();
    }

    @Test
    void returns_null_for_random_string() {
        assertThat(ResourceIdParser.tryParse("not a resource id")).isNull();
    }

    // ---------- slash-FS grammar --------------------------------------------

    @Test
    void parses_slash_fs_with_single_segment_instance() {
        ResourceIdParser.Parsed p = ResourceIdParser.tryParse(
                "snmp/fs/selfmonitor/1/opennms-jvm/OpenNMS_Name_Notifd");
        assertThat(p).isNotNull();
        assertThat(p.nodeId()).isEqualTo("selfmonitor:1");
        assertThat(p.resourceType()).isEqualTo("opennms-jvm");
        assertThat(p.resourceInstance()).isEqualTo("OpenNMS_Name_Notifd");
    }

    @Test
    void parses_slash_fs_with_dotted_instance() {
        ResourceIdParser.Parsed p = ResourceIdParser.tryParse(
                "snmp/fs/selfmonitor/1/jmx-minion/java.lang_type_Memory");
        assertThat(p).isNotNull();
        assertThat(p.nodeId()).isEqualTo("selfmonitor:1");
        assertThat(p.resourceType()).isEqualTo("jmx-minion");
        assertThat(p.resourceInstance()).isEqualTo("java.lang_type_Memory");
    }

    @Test
    void parses_slash_fs_with_multi_segment_instance_preserved() {
        ResourceIdParser.Parsed p = ResourceIdParser.tryParse(
                "snmp/fs/selfmonitor/1/group/a/b/c");
        assertThat(p).isNotNull();
        assertThat(p.nodeId()).isEqualTo("selfmonitor:1");
        assertThat(p.resourceType()).isEqualTo("group");
        // Instance captures everything after the group segment, preserving slashes.
        assertThat(p.resourceInstance()).isEqualTo("a/b/c");
    }

    // ---------- slash-DB grammar --------------------------------------------

    @Test
    void parses_slash_db_with_numeric_node_id() {
        ResourceIdParser.Parsed p = ResourceIdParser.tryParse("snmp/42/hrStorageIndex/1");
        assertThat(p).isNotNull();
        assertThat(p.nodeId()).isEqualTo("42");
        assertThat(p.resourceType()).isEqualTo("hrStorageIndex");
        assertThat(p.resourceInstance()).isEqualTo("1");
    }

    @Test
    void parses_slash_db_with_multi_segment_instance() {
        ResourceIdParser.Parsed p = ResourceIdParser.tryParse("snmp/42/group/a/b/c");
        assertThat(p).isNotNull();
        assertThat(p.nodeId()).isEqualTo("42");
        assertThat(p.resourceType()).isEqualTo("group");
        assertThat(p.resourceInstance()).isEqualTo("a/b/c");
    }

    // ---------- slash-path fall-through -------------------------------------

    @Test
    void slash_db_non_numeric_first_segment_falls_through() {
        // `nonsense` is not numeric, so SLASH_DB does not match; no other grammar
        // accepts this shape either.
        assertThat(ResourceIdParser.tryParse("snmp/nonsense/x/y")).isNull();
    }

    @Test
    void slash_fs_missing_segments_return_null() {
        assertThat(ResourceIdParser.tryParse("snmp/fs/onlyfs")).isNull();
        assertThat(ResourceIdParser.tryParse("snmp/fs/onlyfs/onlyfid")).isNull();
        assertThat(ResourceIdParser.tryParse("snmp/fs/onlyfs/onlyfid/onlygroup")).isNull();
    }

    @Test
    void slash_db_missing_segments_return_null() {
        assertThat(ResourceIdParser.tryParse("snmp/42")).isNull();
        assertThat(ResourceIdParser.tryParse("snmp/42/onlygroup")).isNull();
    }

    @Test
    void snmp_alone_returns_null() {
        assertThat(ResourceIdParser.tryParse("snmp/")).isNull();
        assertThat(ResourceIdParser.tryParse("snmp")).isNull();
    }
}
