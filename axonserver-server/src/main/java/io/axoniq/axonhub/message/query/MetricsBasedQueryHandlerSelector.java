package io.axoniq.axonhub.message.query;

import io.axoniq.axonhub.metric.ClusterMetric;

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
    public String select(QueryDefinition queryDefinition, String componentName, NavigableSet<String> queryHandlers) {
        return queryHandlers.stream().map(h -> new QueryHandlerWithHistogram(h, metricsRegistry.clusterMetric(queryDefinition, h)))
                .min(this::compareMean)
                .map(q -> q.handler)
                .orElse(null);
    }

//    private int comparePercentile95(QueryHandlerWithHistogram queryHandlerWithSnapshot, QueryHandlerWithHistogram queryHandlerWithSnapshot1) {
//        if( queryHandlerWithSnapshot.snapshot == null) {
//            if( queryHandlerWithSnapshot1.snapshot == null) return queryHandlerWithSnapshot.handler.getClientName().compareTo(queryHandlerWithSnapshot1.handler.getClientName());
//            return 1;
//        }
//
//        if( queryHandlerWithSnapshot1.snapshot == null) return -1;
//        int  p = Double.compare(queryHandlerWithSnapshot.snapshot.get95thPercentile(), queryHandlerWithSnapshot1.snapshot.get95thPercentile());
//        if( p == 0) {
//            p = Long.compare(queryHandlerWithSnapshot.histogram.getCount(), queryHandlerWithSnapshot1.histogram.getCount());
//        }
//        if( p == 0) p = queryHandlerWithSnapshot.handler.getClientName().compareTo(queryHandlerWithSnapshot1.handler.getClientName());
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
//            if( queryHandlerWithSnapshot1.histogram == null) return queryHandlerWithSnapshot.handler.getClientName().compareTo(queryHandlerWithSnapshot1.handler.getClientName());
//            return 1;
//        }
//        if( queryHandlerWithSnapshot1.histogram == null) return -1;
//
//        int  p = Long.compare(queryHandlerWithSnapshot.histogram.getCount(), queryHandlerWithSnapshot1.histogram.getCount());
//        if( p == 0) p = queryHandlerWithSnapshot.handler.getClientName().compareTo(queryHandlerWithSnapshot1.handler.getClientName());
//        return p;
//    }

    private class QueryHandlerWithHistogram  {
        private final String handler;
        private final ClusterMetric clusterMetric;

        private QueryHandlerWithHistogram(String handler, ClusterMetric clusterMetric) {
            this.handler = handler;
            this.clusterMetric = clusterMetric;
        }

    }
}
