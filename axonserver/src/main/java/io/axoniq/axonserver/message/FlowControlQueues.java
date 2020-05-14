/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricName;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public class FlowControlQueues<T> {
    private static final Logger logger = LoggerFactory.getLogger(FlowControlQueues.class);
    private static final AtomicLong requestId = new AtomicLong(0);
    private final Comparator<T> comparator;
    private final int softLimit;
    private final int hardLimit;
    private final MeterFactory meterFactory;
    private final String name;

    private final Map<String, BlockingQueue<FilterNode>> segments = new ConcurrentHashMap<>();
    private final Map<String, Gauge> gauges = new ConcurrentHashMap<>();

    public FlowControlQueues(Comparator<T> comparator, int softLimit, String name, MeterFactory meterFactory) {
        this.comparator = comparator;
        this.softLimit = softLimit;
        this.hardLimit = (int) Math.ceil(softLimit * 1.1);
        this.name = name;
        this.meterFactory = meterFactory;
    }

    public FlowControlQueues(Comparator<T> comparator) {
        this(comparator, 10_000, "queue", null);
    }

    public FlowControlQueues() {
        this(null);
    }

    public T take(String filterValue) throws InterruptedException {
        BlockingQueue<FilterNode> filterSegment = segments.computeIfAbsent(filterValue, this::newQueueWithMetrics);
        FilterNode message = filterSegment.poll(1, TimeUnit.SECONDS);
        return message == null ? null : message.value;
    }

    public void put(String filterValue, T value) {
        put(filterValue, value, 0);
    }

    public void put(String filterValue, T value, long priority) {
        if (value == null) {
            throw new NullPointerException();
        }
        BlockingQueue<FilterNode> filterSegment = segments.computeIfAbsent(filterValue, this::newQueueWithMetrics);
        if (filterSegment.size() >= hardLimit) {
            logger.warn("Reached hard limit on queue size of {}, priority of item failed to be added {}, hard limit {}.",
                        filterSegment.size(),
                        priority,
                        hardLimit);
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to add request to queue " + filterValue);
        }
        if (priority <= 0 && filterSegment.size() >= softLimit) {
            logger.warn("Reached soft limit on queue size of {}, priority of item failed to be added {}, soft limit {}.",
                        filterSegment.size(),
                        priority,
                        softLimit);
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to add request to queue " + filterValue);
        }
        FilterNode filterNode = new FilterNode(value);
        if (!filterSegment.offer(filterNode)) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to add request to queue " + filterValue);
        }

        if (logger.isTraceEnabled()) {
            filterSegment.forEach(node -> logger.trace("entry: {}", node.id));
        }
    }

    public void move(String oldFilterValue, Function<T, String> newFilterAssignment) {
        logger.debug("Remove: {}", oldFilterValue);
        BlockingQueue<FilterNode> filterSegment = segments.remove(oldFilterValue);
        if (filterSegment == null) {
            return;
        }
        Optional.ofNullable(gauges.remove(oldFilterValue))
                .ifPresent(Meter::close);

        filterSegment.forEach(filterNode -> {
            String newFilterValue = newFilterAssignment.apply(filterNode.value);
            if (newFilterValue != null) {
                try {
                    segments.computeIfAbsent(newFilterValue, this::newQueueWithMetrics).put(filterNode);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("Interrupt during move");
                    throw new MessagingPlatformException(ErrorCode.OTHER,
                                                         "Failed to add request to queue " + newFilterValue);
                }
            }
        });
    }

    private PriorityBlockingQueue<FilterNode> newQueueWithMetrics(String filterValue) {
        PriorityBlockingQueue<FilterNode> queue = new PriorityBlockingQueue<>();
        Optional.ofNullable(meterFactory)
                .ifPresent(mf -> {
                    Gauge gauge = mf.gauge(new ItemMetricName(filterValue), queue, BlockingQueue::size);
                    gauges.put(filterValue, gauge);
                });
        return queue;
    }

    public Map<String, BlockingQueue<FilterNode>> getSegments() {
        return segments;
    }

    private class ItemMetricName implements MetricName {

        private final String itemName;

        private ItemMetricName(String itemName) {
            this.itemName = itemName;
        }

        @Override
        public String metric() {
            return "axon." + name + "." + itemName + ".size";
        }

        @Override
        public String description() {
            return "The size of queue holding messages to be picked up by Axon Server for processing per client";
        }
    }

    public class FilterNode implements Comparable<FilterNode> {
        private final T value;
        private final long id;

        public FilterNode(T value) {
            this.value = value;
            this.id = requestId.getAndIncrement();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilterNode that = (FilterNode) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public int compareTo(FilterNode o) {
            if( comparator != null) {
                int rc = comparator.compare(value, o.value);
                if( rc != 0) return rc;

            }
            return Long.compare(id, o.id);


        }
    }

}
