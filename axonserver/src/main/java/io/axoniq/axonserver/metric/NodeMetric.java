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
 * Created by Sara Pellegrini on 17/04/2018.
 * sara.pellegrini@gmail.com
 */
public class NodeMetric implements ClusterMetric{

    private final AxonServerMetric metric;

    public NodeMetric(AxonServerMetric metric) {
        this.metric = metric;
    }

    @Override
    public double value() {
        return metric.getValue();
    }

    @Override
    public double min() {
        return metric.getMin();
    }

    @Override
    public double max() {
        return metric.getMax();
    }

    @Override
    public double mean() {
        return metric.getMean();
    }

    @Override
    public long count() {
        return metric.getCount();
    }
}
