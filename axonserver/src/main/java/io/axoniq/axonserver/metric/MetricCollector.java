package io.axoniq.axonserver.metric;

import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public interface MetricCollector extends Function<String, ClusterMetric>  {

    Iterable<AxonServerMetric> getAll();
}
