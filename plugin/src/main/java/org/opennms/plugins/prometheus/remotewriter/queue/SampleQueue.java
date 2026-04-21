/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * Bounded in-memory queue of {@link MappedSample}s awaiting flush.
 *
 * <p>{@link #enqueue(MappedSample)} never blocks. If the queue has no capacity,
 * the method throws {@link StorageException} and the sample is reported as
 * dropped via {@link #getSamplesDroppedQueueFull()}. This is the v0.1
 * backpressure contract: the plugin pushes the signal back to OpenNMS rather
 * than silently absorbing overruns.
 *
 * <p>v0.1 has no durable on-disk buffer — samples are lost on process restart.
 * A WAL is deferred to v0.2.
 */
public final class SampleQueue {

    private final ArrayBlockingQueue<MappedSample> queue;
    private final AtomicLong samplesEnqueued        = new AtomicLong();
    private final AtomicLong samplesDequeued        = new AtomicLong();
    private final AtomicLong samplesDroppedQueueFull = new AtomicLong();

    public SampleQueue(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("queue capacity must be >= 1, got " + capacity);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Enqueue a sample for later flush. Throws when the queue is full — the
     * caller (OpenNMS {@code store()}) sees the failure and can react.
     */
    public void enqueue(MappedSample sample) throws StorageException {
        if (!queue.offer(sample)) {
            samplesDroppedQueueFull.incrementAndGet();
            throw new StorageException(
                "prometheus-remote-writer queue full (capacity=" + queue.remainingCapacity()
                    + " + " + queue.size() + "); dropping sample");
        }
        samplesEnqueued.incrementAndGet();
    }

    /**
     * Wait up to {@code timeout} for a sample; on arrival, additionally drain
     * up to {@code maxBatch - 1} more samples without blocking. Returns the
     * accumulated list, which is empty when the timeout elapses with an
     * empty queue.
     */
    public List<MappedSample> pollBatch(int maxBatch, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (maxBatch < 1) throw new IllegalArgumentException("maxBatch must be >= 1");
        MappedSample head = queue.poll(timeout, unit);
        if (head == null) return List.of();

        List<MappedSample> batch = new java.util.ArrayList<>(maxBatch);
        batch.add(head);
        queue.drainTo(batch, maxBatch - 1);
        samplesDequeued.addAndGet(batch.size());
        return batch;
    }

    /** Drain up to {@code maxBatch} samples without blocking. */
    public List<MappedSample> drain(int maxBatch) {
        List<MappedSample> batch = new java.util.ArrayList<>(maxBatch);
        queue.drainTo(batch, maxBatch);
        samplesDequeued.addAndGet(batch.size());
        return batch;
    }

    public int depth()    { return queue.size(); }
    public int capacity() { return queue.size() + queue.remainingCapacity(); }

    public long getSamplesEnqueued()         { return samplesEnqueued.get(); }
    public long getSamplesDequeued()         { return samplesDequeued.get(); }
    public long getSamplesDroppedQueueFull() { return samplesDroppedQueueFull.get(); }
}
