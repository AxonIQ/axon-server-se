package io.axoniq.axonserver.metric;

import io.axoniq.axonserver.grpc.internal.Metric;


/**
 * Created by Sara Pellegrini on 17/04/2018.
 * sara.pellegrini@gmail.com
 */
public class NodeMetric implements ClusterMetric{

    private final Metric metric;

    public NodeMetric(Metric metric) {
        this.metric = metric;
    }

    @Override
    public long size() {
        return metric.getSize();
    }

    @Override
    public long min() {
        return metric.getMin();
    }

    @Override
    public long max() {
        return metric.getMax();
    }

    @Override
    public double mean() {
        return metric.getMean();
    }
}
