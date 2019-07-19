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
 * Maintains metrics values for a sliding time window. Keeps buckets per N milliseconds and provides operation to calculate
 * totals over a specific period.
 * @author Marc Gathier
 * @since 4.2
 */
public class SlidingWindow<T> {
    // keeps values per bucketMs milliseconds
    private final NavigableMap<Long, T> buckets = new ConcurrentSkipListMap<>();

    // time period for the individual bucket
    private final long bucketMs;
    // operation used to aggregate values
    private final BinaryOperator<T> aggregateOperation;
    // Supplier for a new value
    private final Supplier<T> createOperation;
    protected final Clock clock;
    private final long maxBuckets;

    public SlidingWindow(Supplier<T> createOperation, BinaryOperator<T> aggregateOperation, Clock clock) {
        this(5, 1200, TimeUnit.SECONDS, createOperation, aggregateOperation, clock);
    }
    public SlidingWindow(int bucketSize, int history, TimeUnit timeUnit, Supplier<T> createOperation, BinaryOperator<T> aggregateOperation, Clock clock) {
        Assert.isTrue(bucketSize > 0, "Bucketsize must be > 0");
        this.aggregateOperation = aggregateOperation;
        this.createOperation = createOperation;

        this.clock = clock;
        this.bucketMs = timeUnit.toMillis(bucketSize);
        this.maxBuckets = (long)Math.ceil(timeUnit.toMillis(history)/(float)this.bucketMs);
    }

    /**
     * Retrieves current metric value. Creates one if no value exists for current bucket. Removes old buckets if necessary.
     * @return metric value for current timestamp
     */
    public T current() {
        long key = bucket(clock.millis());
        AtomicBoolean updated = new AtomicBoolean();
        T bucket = buckets.computeIfAbsent(key, k -> {
            updated.set(true);
            return createOperation.get();
        });
        if( updated.get()) {
            truncate(key- maxBuckets);
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

    /**
     * Calculates the aggregated metric value over the specified time period.
     * @param time value for period
     * @param timeUnit unit for value
     * @return aggregated metric
     */
    public T aggregate(long time, TimeUnit timeUnit) {
        long minKey = (long)((double)clock.millis()-timeUnit.toMillis(time))/bucketMs;
        return buckets.tailMap(minKey, true).values().stream().reduce(createOperation.get(), aggregateOperation);
    }
}
