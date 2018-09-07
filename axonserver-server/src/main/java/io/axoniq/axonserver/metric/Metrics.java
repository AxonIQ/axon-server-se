package io.axoniq.axonserver.metric;

import io.axoniq.axonserver.cluster.ClusterMetricTarget;

import java.util.Iterator;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 17/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Metrics implements Iterable<ClusterMetric> {

    private String metricName;

    private ClusterMetricTarget target;

    public Metrics(String metricName, ClusterMetricTarget target) {
        this.metricName = metricName;
        this.target = target;
    }

    @Override
    public Iterator<ClusterMetric> iterator() {
        return stream(target.getAll().spliterator(), false)
                .filter(metric -> metric.getName().equals(metricName))
                .map(metric -> (ClusterMetric) new NodeMetric(metric))
                .iterator();
    }
}
