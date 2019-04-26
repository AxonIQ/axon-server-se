/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import org.springframework.util.Assert;

import java.time.Clock;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Marc Gathier
 */
public class SlidingWindow<T> {
    private final NavigableMap<Long, T> buckets = new ConcurrentSkipListMap<>();

    private final long bucketMs;
    private final BinaryOperator<T> reduceOperation;
    private final Supplier<T> createOperation;
    private final Clock clock;
    private final long history;

    public SlidingWindow(Supplier<T> createOperation, BinaryOperator<T> reduceOperation, Clock clock) {
        this(5, 1200, TimeUnit.SECONDS, createOperation, reduceOperation, clock);
    }
    public SlidingWindow(int bucketSize, int history, TimeUnit timeUnit, Supplier<T> createOperation, BinaryOperator<T> reduceOperation, Clock clock) {
        Assert.isTrue(bucketSize > 0, "Bucketsize must be > 0");
        this.reduceOperation = reduceOperation;
        this.createOperation = createOperation;

        this.clock = clock;
        this.bucketMs = timeUnit.toMillis(bucketSize);
        this.history = (long)Math.ceil(timeUnit.toMillis(history)/(float)this.bucketMs);
    }

    public T current() {
        long key = bucket(clock.millis());
        AtomicBoolean updated = new AtomicBoolean();
        T bucket = buckets.computeIfAbsent(key, k -> {
            updated.set(true);
            return createOperation.get();
        });
        if( updated.get()) {
            truncate(key-history);
        }
        return bucket;
    }

    long bucket(long timestamp) {
        return (long)Math.floor((double)timestamp/bucketMs);
    }

    private void truncate(long minKey) {
        Long firstKey = buckets.firstKey();
        while( firstKey != null && firstKey < minKey) {
            buckets.remove(firstKey);
            firstKey = buckets.firstKey();
        }
    }

    public T reduce(long time, TimeUnit timeUnit) {
        long minKey = (long)((double)clock.millis()-timeUnit.toMillis(time))/bucketMs;
        return buckets.tailMap(minKey, true).values().stream().reduce(createOperation.get(), reduceOperation);
    }
}
