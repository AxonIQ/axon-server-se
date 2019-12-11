package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.internal.Metric;
import io.axoniq.axonserver.metric.AxonServerMetric;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public class GrpcBackedMetric implements AxonServerMetric {
    private Metric metric;

    public GrpcBackedMetric(Metric metric) {
        this.metric = metric;
    }

    @Override
    public double getValue() {
        return metric.getValue();
    }

    @Override
    public double getMin() {
        return metric.getMin();
    }

    @Override
    public double getMax() {
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

    @Override
    public long getCount() {
        return metric.getCount();
    }
}
