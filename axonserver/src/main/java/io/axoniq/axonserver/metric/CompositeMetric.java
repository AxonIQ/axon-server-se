/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by Sara Pellegrini on 18/04/2018.
 * sara.pellegrini@gmail.com
 */
public class CompositeMetric implements ClusterMetric {

    private final Collection<ClusterMetric> clusterMetrics = new LinkedList<>();

    public CompositeMetric(ClusterMetric ... clusterMetric) {
        this(Arrays.asList(clusterMetric));
    }

    public CompositeMetric(ClusterMetric clusterMetric, Iterable<ClusterMetric> clusterMetrics) {
        this.clusterMetrics.add(clusterMetric);
        clusterMetrics.forEach(this.clusterMetrics::add);
    }

    public CompositeMetric(Iterable<ClusterMetric> clusterMetrics) {
        clusterMetrics.forEach(this.clusterMetrics::add);
    }

    @Override
    public double value() {
        return clusterMetrics.stream().map(ClusterMetric::value).reduce(Double::sum).orElse(0d);
    }

    @Override
    public long count() {
        return clusterMetrics.stream().map(ClusterMetric::count).reduce(Long::sum).orElse(0L);
    }

    @Override
    public double min() {
        return clusterMetrics.stream().map(ClusterMetric::min).min(Double::compareTo).orElse(0d);
    }

    @Override
    public double max() {
        return clusterMetrics.stream().map(ClusterMetric::max).max(Double::compareTo).orElse(0d);
    }

    @Override
    public double mean() {
        return clusterMetrics.stream().map(metric -> metric.mean() * metric.count())
                             .reduce(Double::sum)
                             .orElse((double) 0)
                / count();
    }

    @Override
    public String toString() {
        return String.format("value: %d, mean: %f", value(), mean());
    }
}
