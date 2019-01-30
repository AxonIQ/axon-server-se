package io.axoniq.axonserver.enterprise.messaging.query;

import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.metric.ClusterMetric;

import java.util.NavigableSet;

/**
 * Author: marc
 */
public class MetricsBasedQueryHandlerSelector implements QueryHandlerSelector {
    private final QueryMetricsRegistry metricsRegistry;

    public MetricsBasedQueryHandlerSelector(QueryMetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
    }

    @Override
    public ClientIdentification select(QueryDefinition queryDefinition, String componentName, NavigableSet<ClientIdentification> queryHandlers) {
        return queryHandlers.stream().map(h -> new QueryHandlerWithHistogram(h, metricsRegistry.clusterMetric(queryDefinition, h)))
                .min(this::compareMean)
                .map(q -> q.handler)
                .orElse(null);
    }

//    private int comparePercentile95(QueryHandlerWithHistogram queryHandlerWithSnapshot, QueryHandlerWithHistogram queryHandlerWithSnapshot1) {
//        if( queryHandlerWithSnapshot.snapshot == null) {
//            if( queryHandlerWithSnapshot1.snapshot == null) return queryHandlerWithSnapshot.handler.getClient().compareTo(queryHandlerWithSnapshot1.handler.getClient());
//            return 1;
//        }
//
//        if( queryHandlerWithSnapshot1.snapshot == null) return -1;
//        int  p = Double.compare(queryHandlerWithSnapshot.snapshot.get95thPercentile(), queryHandlerWithSnapshot1.snapshot.get95thPercentile());
//        if( p == 0) {
//            p = Long.compare(queryHandlerWithSnapshot.histogram.getCount(), queryHandlerWithSnapshot1.histogram.getCount());
//        }
//        if( p == 0) p = queryHandlerWithSnapshot.handler.getClient().compareTo(queryHandlerWithSnapshot1.handler.getClient());
//        return p;
//    }
    private int compareMean(QueryHandlerWithHistogram queryHandlerWithSnapshot, QueryHandlerWithHistogram queryHandlerWithSnapshot1) {
        if( queryHandlerWithSnapshot.clusterMetric == null) {
            if( queryHandlerWithSnapshot1.clusterMetric == null) return queryHandlerWithSnapshot.handler.compareTo(queryHandlerWithSnapshot1.handler);
            return 1;
        }

        if( queryHandlerWithSnapshot1.clusterMetric == null) return -1;
        int  p = 0;

        if( queryHandlerWithSnapshot.clusterMetric.size() > 20 && queryHandlerWithSnapshot1.clusterMetric.size() > 20 )
            p = Double.compare(queryHandlerWithSnapshot.clusterMetric.mean(), queryHandlerWithSnapshot1.clusterMetric.mean());

        if( p == 0) {
            p = Long.compare(queryHandlerWithSnapshot.clusterMetric.size(), queryHandlerWithSnapshot1.clusterMetric.size());
        }
        if( p == 0) p = queryHandlerWithSnapshot.handler.compareTo(queryHandlerWithSnapshot1.handler);
        return p;
    }

//    private int compareCount(QueryHandlerWithHistogram queryHandlerWithSnapshot, QueryHandlerWithHistogram queryHandlerWithSnapshot1) {
//        if( queryHandlerWithSnapshot.histogram == null) {
//            if( queryHandlerWithSnapshot1.histogram == null) return queryHandlerWithSnapshot.handler.getClient().compareTo(queryHandlerWithSnapshot1.handler.getClient());
//            return 1;
//        }
//        if( queryHandlerWithSnapshot1.histogram == null) return -1;
//
//        int  p = Long.compare(queryHandlerWithSnapshot.histogram.getCount(), queryHandlerWithSnapshot1.histogram.getCount());
//        if( p == 0) p = queryHandlerWithSnapshot.handler.getClient().compareTo(queryHandlerWithSnapshot1.handler.getClient());
//        return p;
//    }

    private class QueryHandlerWithHistogram  {
        private final ClientIdentification handler;
        private final ClusterMetric clusterMetric;

        private QueryHandlerWithHistogram(ClientIdentification handler, ClusterMetric clusterMetric) {
            this.handler = handler;
            this.clusterMetric = clusterMetric;
        }

    }
}
