/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.wire;

import java.util.Collection;
import java.util.function.Function;

import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;

/**
 * Selects between the v1 and v2 Remote Write builders based on the
 * operator-configured {@code wire.protocol-version}. The two builders
 * have different output protobufs but the same {@link BuildResult}
 * record shape (compressed payload + counters), so callers can treat
 * them uniformly.
 *
 * <p>Returned function is stateless and safe to share across threads —
 * each call constructs a fresh request.
 */
public final class RemoteWriteRequestBuilders {

    private RemoteWriteRequestBuilders() { /* utility */ }

    /**
     * @param protocolVersion {@code 1} or {@code 2}
     * @return a builder that turns a sample collection into a
     *         {@link BuildResult}
     * @throws IllegalArgumentException if {@code protocolVersion} is
     *         neither 1 nor 2
     */
    public static Function<Collection<MappedSample>, BuildResult> forVersion(int protocolVersion) {
        return switch (protocolVersion) {
            case 1 -> RemoteWriteRequestBuilder::build;
            case 2 -> RemoteWriteV2RequestBuilder::build;
            default -> throw new IllegalArgumentException(
                "wire.protocol-version must be 1 or 2, got: " + protocolVersion);
        };
    }
}
