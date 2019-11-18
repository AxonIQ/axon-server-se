/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

/**
 * Created by Sara Pellegrini on 18/04/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeClusterMetric implements ClusterMetric {

    private final long size;
    private final long min;
    private final long max;
    private final double mean;

    public FakeClusterMetric(long size) {
        this(size, 0L, 0L, 0);
    }

    public FakeClusterMetric(long size, double mean) {
        this(size,0L,0L, mean);
    }

    public FakeClusterMetric(long size, long min, long max, double mean) {
        this.size = size;
        this.min = min;
        this.max = max;
        this.mean = mean;
    }

    @Override
    public long value() {
        return size;
    }

    @Override
    public long min() {
        return min;
    }

    @Override
    public long max() {
        return max;
    }

    @Override
    public double mean() {
        return mean;
    }

    @Override
    public double doubleValue() {
        return 0;
    }
}