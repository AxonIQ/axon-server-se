package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.MetricsEvents;
import io.axoniq.axonserver.internal.grpc.Metric;
import io.axoniq.axonserver.internal.grpc.NodeMetrics;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.metric.NodeMetric;
import org.springframework.context.event.EventListener;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 17/04/2018.
 * sara.pellegrini@gmail.com
 */
public class ClusterMetricTarget implements MetricCollector {

    private final Map<String, Collection<Metric>> clusterMetricMap = new ConcurrentHashMap<>();

    public Iterable<Metric> getAll() {
        return clusterMetricMap.entrySet().stream()
                               .map(Map.Entry::getValue)
                               .flatMap(Collection::stream)
                               .collect(Collectors.toList());
    }

    @EventListener
    public void on(MetricsEvents.MetricsChanged metricsChanged) {
        NodeMetrics nodeMetrics = metricsChanged.nodeMetrics();
        clusterMetricMap.put(nodeMetrics.getNode(), nodeMetrics.getMetricsList());
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceDisconnected axonHubInstanceDisconnected){
        this.clusterMetricMap.remove(axonHubInstanceDisconnected.getNodeName());
    }

    @Override
    public ClusterMetric apply(@Nonnull String metricName) {
        Set<ClusterMetric> metrics = stream(getAll().spliterator(), false)
                .filter(metric -> metricName.equals(metric.getName()))
                .map(NodeMetric::new)
                .collect(Collectors.toSet());
        return new CompositeMetric(metrics);
    }
}
