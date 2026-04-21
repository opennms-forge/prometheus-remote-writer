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
}
