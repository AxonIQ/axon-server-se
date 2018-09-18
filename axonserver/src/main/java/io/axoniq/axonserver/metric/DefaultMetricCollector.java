package io.axoniq.axonserver.metric;

import io.axoniq.axonserver.internal.grpc.Metric;
import java.util.Collections;

/**
 * Author: marc
 */
public class DefaultMetricCollector implements MetricCollector {

    @Override
    public Iterable<Metric> getAll() {
        return Collections.emptyList();
    }

    @Override
    public ClusterMetric apply(String s) {
        return new CounterMetric(s, () -> 0L);
    }
}
