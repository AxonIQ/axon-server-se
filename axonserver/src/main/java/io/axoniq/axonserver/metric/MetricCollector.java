package io.axoniq.axonserver.metric;

import java.util.function.Function;

/**
 * Author: marc
 */
public interface MetricCollector extends Function<String, ClusterMetric>  {

    Iterable<AxonServerMetric> getAll();
}
