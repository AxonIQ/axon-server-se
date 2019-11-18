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
public class SnapshotMetric implements ClusterMetric {

    private final long size;
    private final long max;
    private final double mean;

    public SnapshotMetric(long size, long max, double mean) {
        this.size = size;
        this.max = max;
        this.mean = mean;
    }

    @Override
    public long value() {
        return size;
    }

    @Override
    public long min() {
        return 0L;
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
