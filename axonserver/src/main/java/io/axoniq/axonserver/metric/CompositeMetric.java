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
    public long size() {
        return clusterMetrics.stream().map(ClusterMetric::size).reduce(Long::sum).orElse(0L);
    }

    @Override
    public long min() {
        return clusterMetrics.stream().map(ClusterMetric::min).min(Long::compareTo).orElse(0L);
    }

    @Override
    public long max() {
        return clusterMetrics.stream().map(ClusterMetric::max).max(Long::compareTo).orElse(0L);
    }

    @Override
    public double mean() {
        return clusterMetrics.stream().map(metric -> metric.mean() * metric.size()).reduce(Double::sum)
                             .orElse((double) 0) / size();
    }
}
