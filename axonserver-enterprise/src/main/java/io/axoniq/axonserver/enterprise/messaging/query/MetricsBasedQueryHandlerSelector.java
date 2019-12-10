package io.axoniq.axonserver.enterprise.messaging.query;

import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.metric.ClusterMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NavigableSet;

/**
 * Selects a handler for a query for a specific component based on the metrics.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class MetricsBasedQueryHandlerSelector implements QueryHandlerSelector {

    private final Logger logger = LoggerFactory.getLogger(MetricsBasedQueryHandlerSelector.class);
    private final QueryMetricsRegistry metricsRegistry;

    public MetricsBasedQueryHandlerSelector(QueryMetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
    }

    @Override
    public ClientIdentification select(QueryDefinition queryDefinition, String componentName,
                                       NavigableSet<ClientIdentification> queryHandlers) {
        return queryHandlers.stream().map(h -> new QueryHandlerWithHistogram(h,
                                                                             metricsRegistry.clusterMetric(
                                                                                     queryDefinition,
                                                                                     h)))
                            .min(this::compareMean)
                            .map(q -> q.handler)
                            .orElse(null);
    }

    private int compareMean(QueryHandlerWithHistogram queryHandlerWithSnapshot,
                            QueryHandlerWithHistogram queryHandlerWithSnapshot1) {
        logger.trace("Comparing handlers: {} and {}, clusterMetrics: {} and {}",
                     queryHandlerWithSnapshot.handler.getClient(),
                     queryHandlerWithSnapshot1.handler.getClient(),
                     queryHandlerWithSnapshot.clusterMetric,
                     queryHandlerWithSnapshot1.clusterMetric);
        if (queryHandlerWithSnapshot.clusterMetric == null) {
            if (queryHandlerWithSnapshot1.clusterMetric == null) {
                return queryHandlerWithSnapshot.handler.compareTo(
                        queryHandlerWithSnapshot1.handler);
            }
            return 1;
        }

        if (queryHandlerWithSnapshot1.clusterMetric == null) {
            return -1;
        }
        int p = 0;

        if (queryHandlerWithSnapshot.clusterMetric.count() > 20
                && queryHandlerWithSnapshot1.clusterMetric.count() > 20) {
            p = Double.compare(queryHandlerWithSnapshot.clusterMetric.mean(),
                               queryHandlerWithSnapshot1.clusterMetric.mean());
        }

        if (p == 0) {
            p = Long.compare(queryHandlerWithSnapshot.clusterMetric.count(),
                             queryHandlerWithSnapshot1.clusterMetric.count());
        }
        if (p == 0) {
            p = queryHandlerWithSnapshot.handler.compareTo(queryHandlerWithSnapshot1.handler);
        }
        return p;
    }


    private class QueryHandlerWithHistogram {

        private final ClientIdentification handler;
        private final ClusterMetric clusterMetric;

        private QueryHandlerWithHistogram(ClientIdentification handler, ClusterMetric clusterMetric) {
            this.handler = handler;
            this.clusterMetric = clusterMetric;
        }
    }
}
