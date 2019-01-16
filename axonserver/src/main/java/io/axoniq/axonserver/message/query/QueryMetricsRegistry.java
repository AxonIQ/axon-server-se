package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.metric.Metrics;
import io.axoniq.axonserver.metric.SnapshotMetric;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/**
 * Author: marc
 */
@Service("QueryMetricsRegistry")
public class QueryMetricsRegistry {
    private final Logger logger = LoggerFactory.getLogger(QueryMetricsRegistry.class);
    private final Map<String, Timer> timerMap = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;
    private final MetricCollector clusterMetrics;

    public QueryMetricsRegistry(MeterRegistry meterRegistry, MetricCollector clusterMetricTarget) {
        this.meterRegistry = meterRegistry;
        this.clusterMetrics = clusterMetricTarget;
    }

    public void add(QueryDefinition query, ClientIdentification clientId, long duration) {
        try {
            timer(query, clientId).record(duration, TimeUnit.MILLISECONDS);
        } catch( Exception ex) {
            logger.debug("Failed to create timer", ex);
        }
    }

    public ClusterMetric clusterMetric(QueryDefinition query, ClientIdentification clientId){
        String metricName = metricName(query, clientId);
        return new CompositeMetric(new SnapshotMetric(timer(query, clientId).takeSnapshot()), new Metrics(metricName, clusterMetrics));
    }


    private Timer timer(QueryDefinition query, ClientIdentification clientId) {
        String metricName = metricName(query, clientId);
        return timerMap.computeIfAbsent(metricName, meterRegistry::timer);
    }

    private String metricName(QueryDefinition query, ClientIdentification clientId) {
        return String.format( "axon.query.%s.%s", query.getQueryName(), clientId.metricName());
    }

    public QueryMetric queryMetric(QueryDefinition query, ClientIdentification clientId, String componentName){
        ClusterMetric clusterMetric = clusterMetric(query, clientId);
        return new QueryMetric(query, clientId.metricName(), componentName, clusterMetric.size());
    }

    public Counter counter(String name) {
        return Counter.builder(name).register(meterRegistry);
    }

    public <T> Gauge gauge(String name, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return Gauge.builder(name, objectToWatch, gaugeFunction)
             .register(meterRegistry);
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
