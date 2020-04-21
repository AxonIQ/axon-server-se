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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public class FlowControlQueues<T> {
    private static final Logger logger = LoggerFactory.getLogger(FlowControlQueues.class);

    private final Comparator<T> comparator;

    private final Map<String, BlockingQueue<T>> segments = new ConcurrentHashMap<>();

    public FlowControlQueues(Comparator<T> comparator) {
        this.comparator = comparator;
    }

    public FlowControlQueues() {
        this((t1, t2) -> 0);
    }

    public T take(String filterValue) throws InterruptedException {
        BlockingQueue<T> filterSegment = segments.computeIfAbsent(filterValue, f -> new PriorityBlockingQueue<>(100, comparator));
        return filterSegment.poll(1, TimeUnit.SECONDS);
    }

    public void put(String filterValue, T value) {
        if (value == null) {
            throw new NullPointerException();
        }
        BlockingQueue<T> filterSegment = segments.computeIfAbsent(filterValue, f -> new PriorityBlockingQueue<>(100, comparator));
        if (!filterSegment.offer(value)) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to add request to queue " + filterValue);
        }
    }

    public void move(String oldFilterValue, Function<T, String> newFilterAssignment) {
        logger.debug("Remove: {}", oldFilterValue);
        BlockingQueue<T> filterSegment = segments.remove(oldFilterValue);
        if( filterSegment == null) return;

        filterSegment.forEach(filterNode -> {
            String newFilterValue = newFilterAssignment.apply(filterNode);
            if( newFilterValue != null) {
                try {
                    segments.computeIfAbsent(newFilterValue, f -> new PriorityBlockingQueue<>(100, comparator)).put(filterNode);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("Interrupt during move");
                    throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to add request to queue " + newFilterValue);
                }
            }

        });
    }

    public Map<String, BlockingQueue<T>> getSegments() {
        return segments;
    }


}
