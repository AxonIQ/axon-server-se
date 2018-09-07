package io.axoniq.axonserver.message.query;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.cluster.ClusterMetricTarget;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.Metrics;
import io.axoniq.axonserver.metric.SnapshotMetric;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Author: marc
 */
@Service("QueryMetricsRegistry")
public class QueryMetricsRegistry {
    private final MetricRegistry metricRegistry;
    private final MetricRegistry.MetricSupplier<Histogram> histogramFactory;
    private final ClusterMetricTarget clusterMetrics;

    public QueryMetricsRegistry(MetricRegistry metricRegistry,
                                MetricRegistry.MetricSupplier<Histogram> histogramFactory,
                                ClusterMetricTarget clusterMetricTarget) {
        this.metricRegistry = metricRegistry;
        this.histogramFactory = histogramFactory;
        this.clusterMetrics = clusterMetricTarget;
    }

    public void add(QueryDefinition query, String clientId, long duration) {
        try {
            histogram(query, clientId).update(duration);
        } catch( Exception ignore) {
        }
    }

    ClusterMetric clusterMetric(QueryDefinition query, String clientId){
        String metricName = metricName(query, clientId);
        return new CompositeMetric(new SnapshotMetric(histogram(query, clientId).getSnapshot()), new Metrics(metricName, clusterMetrics));
    }

    public void register(String name,
                      Gauge supplier) {
        metricRegistry.register(name, supplier);
    }

    private Histogram histogram(QueryDefinition query, String clientId) {
        return metricRegistry.histogram(metricName(query, clientId), histogramFactory);
    }

    private String metricName(QueryDefinition query, String clientId) {
        return name("query", query.getQueryName(), clientId);
    }

    public QueryMetric queryMetric(QueryDefinition query, String clientId, String componentName){
        ClusterMetric clusterMetric = clusterMetric(query, clientId);
        return new QueryMetric(query, clientId, componentName, clusterMetric.size());
    }

    long dispatchedCount(){
        String name = "queries";
        return metricRegistry.counter(name).getCount() + new CompositeMetric(new Metrics(name, clusterMetrics)).size();
    }

    void increaseDispatchedCount(){
        metricRegistry.counter("queries").inc();
    }

    @KeepNames
    public static class QueryMetric {
        private final QueryDefinition queryDefinition;
        private final String clientId;
        private final String componentName;
        private final long count;

        QueryMetric(QueryDefinition queryDefinition, String clientId, String componentName, long count) {
            this.queryDefinition = queryDefinition;
            this.clientId = clientId;
            this.componentName = componentName;
            this.count = count;
        }

        public QueryDefinition getQueryDefinition() {
            return queryDefinition;
        }

        public String getClientId() {
            return clientId;
        }

        public String getComponentName() {
            return componentName;
        }

        public long getCount() {
            return count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryMetric that = (QueryMetric) o;
            return Objects.equals(queryDefinition, that.queryDefinition) &&
                    Objects.equals(clientId, that.clientId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryDefinition, clientId);
        }
    }
}
