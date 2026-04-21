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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.timeseries.DataPoint;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;

class PromResponseParserTest {

    // ---------- /series response -------------------------------------------

    @Test
    void series_response_reconstructs_metric_with_intrinsic_name_and_resource_id() {
        String json = """
            {
              "status": "success",
              "data": [
                {
                  "__name__": "ifHCInOctets",
                  "resourceId": "nodeSource[NOC:router-42].interfaceSnmp[eth0]",
                  "node": "NOC:router-42",
                  "if_name": "eth0"
                }
              ]
            }""";
        List<Metric> out = PromResponseParser.parseSeriesResponse(json);

        assertThat(out).hasSize(1);
        Metric m = out.get(0);
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.name).getValue()).isEqualTo("ifHCInOctets");
        assertThat(m.getFirstTagByKey(IntrinsicTagNames.resourceId).getValue())
                .isEqualTo("nodeSource[NOC:router-42].interfaceSnmp[eth0]");
        assertThat(m.getIntrinsicTags()).hasSize(2);
        // "node" and "if_name" go into meta (partition-lossy on round-trip).
        assertThat(m.getMetaTags())
                .extracting("key")
                .containsOnly("node", "if_name");
    }

    @Test
    void series_response_empty_data_returns_empty_list() {
        String json = "{\"status\":\"success\",\"data\":[]}";
        assertThat(PromResponseParser.parseSeriesResponse(json)).isEmpty();
    }

    @Test
    void series_response_error_status_is_rejected() {
        String json = "{\"status\":\"error\",\"error\":\"bad request\"}";
        assertThatThrownBy(() -> PromResponseParser.parseSeriesResponse(json))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bad request");
    }

    // ---------- /query_range response --------------------------------------

    @Test
    void range_response_reconstructs_data_points() {
        String json = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [
                  {
                    "metric": { "__name__": "x" },
                    "values": [
                      [1700000000, "1.5"],
                      [1700000060, "2.5"],
                      [1700000120, "3.5"]
                    ]
                  }
                ]
              }
            }""";
        List<DataPoint> pts = PromResponseParser.parseRangeResponse(json);

        assertThat(pts).hasSize(3);
        assertThat(pts.get(0).getTime().getEpochSecond()).isEqualTo(1_700_000_000L);
        assertThat(pts.get(0).getValue()).isEqualTo(1.5);
        assertThat(pts.get(2).getValue()).isEqualTo(3.5);
    }

    @Test
    void range_response_empty_result_returns_empty_list() {
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[]}}""";
        assertThat(PromResponseParser.parseRangeResponse(json)).isEmpty();
    }

    @Test
    void range_response_parses_fractional_timestamps() {
        String json = """
            {"status":"success","data":{"resultType":"matrix","result":[
              {"metric":{"__name__":"x"},"values":[[1700000000.5,"42"]]}
            ]}}""";
        List<DataPoint> pts = PromResponseParser.parseRangeResponse(json);
        assertThat(pts).hasSize(1);
        // 500 ms offset from the integer second
        assertThat(pts.get(0).getTime().toEpochMilli()).isEqualTo(1_700_000_000_500L);
    }
}
