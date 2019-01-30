package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.internal.Metric;
import io.axoniq.axonserver.metric.AxonServerMetric;

/**
 * Author: marc
 */
public class GrpcBackedMetric implements AxonServerMetric {
    private Metric metric;

    public GrpcBackedMetric(Metric metric) {
        this.metric = metric;
    }

    @Override
    public long getSize() {
        return metric.getSize();
    }

    @Override
    public long getMin() {
        return metric.getMin();
    }

    @Override
    public long getMax() {
        return metric.getMax();
    }

    @Override
    public double getMean() {
        return metric.getMean();
    }

    @Override
    public String getName() {
        return metric.getName();
    }
}
