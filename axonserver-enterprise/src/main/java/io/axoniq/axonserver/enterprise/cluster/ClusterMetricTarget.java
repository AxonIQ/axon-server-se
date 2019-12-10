package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.internal.Metric;
import io.axoniq.axonserver.grpc.internal.NodeMetrics;
import io.axoniq.axonserver.metric.AxonServerMetric;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.metric.NodeMetric;
import io.micrometer.core.instrument.Tags;
import org.springframework.context.event.EventListener;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static java.util.stream.StreamSupport.stream;

/**
 * Collects metrics from other Axon Server nodes.
 * @author Sara Pellegrini
 * @since 4.0
 */
public class ClusterMetricTarget implements MetricCollector {

    private final Map<String, Collection<Metric>> clusterMetricMap = new ConcurrentHashMap<>();

    public Iterable<AxonServerMetric> getAll(String metric, Tags tags) {
        return clusterMetricMap.entrySet().stream()
                               .map(Map.Entry::getValue)
                               .flatMap(Collection::stream)
                               .filter(m -> match(m, metric, tags))
                               .map(GrpcBackedMetric::new)
                               .collect(Collectors.toList());
    }

    private boolean match(Metric m, String metric, Tags tags) {
        return m.getName().equals(metric) && tagsMatch(m.getTagsMap(), tags);
    }

    private boolean tagsMatch(Map<String, String> tagsMap, Tags tags) {
        return tags.stream().allMatch(t -> t.getValue().equals(tagsMap.get(t.getKey())));
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
    public ClusterMetric apply(@Nonnull String metricName, Tags tags) {
        Set<ClusterMetric> metrics = stream(getAll(metricName, tags).spliterator(), false)
                .map(NodeMetric::new)
                .collect(Collectors.toSet());
        return new CompositeMetric(metrics);
    }
}
