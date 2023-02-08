/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
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
    private final MetricName metricName;
    private final ErrorCode errorCode;

    private final Map<String, BlockingQueue<DestinationNode>> segments = new ConcurrentHashMap<>();
    private final Map<String, Gauge> gauges = new ConcurrentHashMap<>();

    public FlowControlQueues(Comparator<T> comparator, int softLimit, MetricName metricName,
                             MeterFactory meterFactory, ErrorCode errorCode) {
        this.comparator = comparator;
        this.softLimit = softLimit;
        this.hardLimit = (int) Math.ceil(softLimit * 1.1);
        this.metricName = metricName;
        this.meterFactory = meterFactory;
        this.errorCode = errorCode;
    }

    public FlowControlQueues(Comparator<T> comparator) {
        this(comparator, 10_000, null, null, ErrorCode.OTHER);
    }

    public FlowControlQueues() {
        this(null);
    }

    public T take(String filterValue) throws InterruptedException {
        BlockingQueue<DestinationNode> destinationSegment = segments.computeIfAbsent(filterValue,
                                                                                     this::newQueueWithMetrics);
        DestinationNode message = destinationSegment.poll(1, TimeUnit.SECONDS);
        return message == null ? null : message.value;
    }

    public void put(String filterValue, T value) {
        put(filterValue, value, 0);
    }

    /**
     * Enqueues the {@code value} to the destination identified by {@code filterValue} with the specified
     * {@code priority}.
     *
     * @param filterValue the identifier of the destination of the value
     * @param value       the item we need to enqueue
     * @param priority    the priority of the item in the queue
     * @return the function to remove the item from the queue
     */
    public Cancellable put(String filterValue, T value, long priority) {
        if (value == null) {
            throw new NullPointerException();
        }
        BlockingQueue<DestinationNode> destinationSegment = segments.computeIfAbsent(filterValue,
                                                                                     this::newQueueWithMetrics);
        String errorMessage = "Failed to add request to queue ";
        if (destinationSegment.size() >= hardLimit) {
            logger.warn(
                    "Reached hard limit on queue {} of size {}, priority of item failed to be added {}, hard limit {}.",
                    filterValue,
                    destinationSegment.size(),
                    priority,
                    hardLimit);
            throw new MessagingPlatformException(errorCode, errorMessage + filterValue);
        }
        if (priority <= 0 && destinationSegment.size() >= softLimit) {
            logger.warn("Reached soft limit on queue size {} of size {}, priority of item failed to be added {}, soft limit {}.",
                        filterValue,
                        destinationSegment.size(),
                        priority,
                        softLimit);
            throw new MessagingPlatformException(errorCode, errorMessage + filterValue);
        }
        DestinationNode destinationNode = new DestinationNode(value);
        if (!destinationSegment.offer(destinationNode)) {
            throw new MessagingPlatformException(errorCode, errorMessage + filterValue);
        }
        logger.trace("Added new item {} to the queue {}",
                     destinationNode.id, filterValue);
        if (logger.isTraceEnabled()) {
            destinationSegment.forEach(node -> logger.trace("entry: {}", node.id));
        }
        return () -> {
            logger.debug("Remove item {} from queue {}.", destinationNode.id, filterValue);
            return destinationSegment.remove(destinationNode);
        };
    }

    public void move(String oldDestinationValue, Function<T, String> newDestinationAssignment) {
        logger.debug("Remove: {}", oldDestinationValue);
        BlockingQueue<DestinationNode> oldDestination = segments.remove(oldDestinationValue);
        Gauge gauge = gauges.remove(oldDestinationValue);
        if (gauge != null) {
            meterFactory.remove(gauge);
        }
        if (oldDestination == null) {
            return;
        }

        oldDestination.forEach(filterNode -> {
            String destination = newDestinationAssignment.apply(filterNode.value);
            if (destination != null) {
                try {
                    segments.computeIfAbsent(destination, this::newQueueWithMetrics).put(filterNode);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("Interrupt during move");
                    throw new MessagingPlatformException(ErrorCode.OTHER,
                                                         "Failed to move request to queue " + destination);
                }
            }
        });
    }

    private PriorityBlockingQueue<DestinationNode> newQueueWithMetrics(String destination) {
        PriorityBlockingQueue<DestinationNode> queue = new PriorityBlockingQueue<>();
        if (meterFactory != null && metricName != null) {
            Gauge gauge = meterFactory.gauge(metricName,
                                             Tags.of("destination", destination),
                                             queue,
                                             BlockingQueue::size);
            gauges.put(destination, gauge);
        }
        return queue;
    }

    public Map<String, BlockingQueue<DestinationNode>> getSegments() {
        return segments;
    }

    public class DestinationNode implements Comparable<DestinationNode> {

        private final T value;
        private final long id;

        public DestinationNode(T value) {
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
            DestinationNode that = (DestinationNode) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public int compareTo(DestinationNode o) {
            if (comparator != null) {
                int rc = comparator.compare(value, o.value);
                if (rc != 0) {
                    return rc;
                }
            }
            return Long.compare(id, o.id);
        }
    }
}
