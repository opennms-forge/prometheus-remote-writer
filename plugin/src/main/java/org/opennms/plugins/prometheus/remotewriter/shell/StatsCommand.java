/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.shell;

import java.io.PrintStream;
import java.util.Map;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.opennms.plugins.prometheus.remotewriter.PrometheusRemoteWriterStorage;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;

/**
 * {@code opennms:prometheus-writer-stats} — print the plugin's Dropwizard
 * counters and gauges. Output is a stable name/value table; no ANSI colours,
 * sortable, grep-friendly.
 *
 * <p>The Karaf shell scanner reads the {@link Command} metadata and routes
 * {@code opennms:prometheus-writer-stats} invocations here. The
 * {@link org.apache.karaf.shell.api.action.lifecycle.Service} annotation is
 * deliberately <i>not</i> applied: this class is registered solely via the
 * Blueprint descriptor so that the {@code storage} dependency is properly
 * injected. Adding {@code @Service} here would cause Karaf's annotation
 * scanner to register a second, uninjected instance and the shell would
 * non-deterministically pick between the two.
 */
@Command(scope = "opennms", name = "prometheus-writer-stats",
         description = "Print prometheus-remote-writer plugin metrics")
public class StatsCommand implements Action {

    private PrometheusRemoteWriterStorage storage;

    public StatsCommand() {}

    /** Constructor used by tests and Blueprint. */
    public StatsCommand(PrometheusRemoteWriterStorage storage) {
        this.storage = storage;
    }

    public void setStorage(PrometheusRemoteWriterStorage storage) {
        this.storage = storage;
    }

    @Override
    public Object execute() {
        render(System.out);
        return null;
    }

    /** Package-private for tests — renders the stats table to the given stream. */
    void render(PrintStream out) {
        if (storage == null) {
            out.println("prometheus-remote-writer is not active");
            return;
        }
        PluginMetrics metrics = storage.getMetrics();
        if (metrics == null) {
            out.println("prometheus-remote-writer has not been started");
            return;
        }

        Map<String, Number> snapshot = metrics.snapshot();
        int maxName = snapshot.keySet().stream().mapToInt(String::length).max().orElse(20);
        String fmt = "  %-" + maxName + "s  %s%n";

        out.println("prometheus-remote-writer metrics");
        out.println("================================");
        for (Map.Entry<String, Number> e : snapshot.entrySet()) {
            out.printf(fmt, e.getKey(), e.getValue());
        }
    }
}
