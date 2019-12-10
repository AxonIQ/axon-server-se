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

    private final double value;
    private final double max;
    private final double mean;
    private final long count;

    public SnapshotMetric(double max, double mean, long count) {
        this.value = mean;
        this.max = max;
        this.mean = mean;
        this.count = count;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    public double min() {
        return 0d;
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
