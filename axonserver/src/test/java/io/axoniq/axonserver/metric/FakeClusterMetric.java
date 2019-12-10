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

    private final long count;
    private final double min;
    private final double max;
    private final double mean;
    private final double value;

    public FakeClusterMetric(long size) {
        this(size, 0, 0, 0, 0);
    }

    public FakeClusterMetric(double value, double mean) {
        this(0, value, 0, 0, mean);
    }

    public FakeClusterMetric(long count, double value, double min, double max, double mean) {
        this.value = value;
        this.count = count;
        this.min = min;
        this.max = max;
        this.mean = mean;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    public double min() {
        return min;
    }

    @Override
    public double max() {
        return max;
    }

    @Override
    public double mean() {
        return mean;
    }

    @Override
    public long count() {
        return count;
    }
}