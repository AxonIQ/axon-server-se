/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricName;
import io.axoniq.axonserver.metric.Metrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * Registry for metrics regarding query execution.
 * @author Marc Gathier
 * @since 4.0
 */
@Service("QueryMetricsRegistry")
public class QueryMetricsRegistry {
    private final Logger logger = LoggerFactory.getLogger(QueryMetricsRegistry.class);
    private final MeterFactory meterFactory;
    private final boolean legacyMetricsEnabled;

    /**
     * Constructor of the registry.
     *
     * @param meterFactory factory to create meters.
     */
    public QueryMetricsRegistry(MeterFactory meterFactory,
                                @Value("${axoniq.axonserver.legacy-metrics-enabled:true}") boolean legacyMetricsEnabled) {
        this.meterFactory = meterFactory;
        this.legacyMetricsEnabled = legacyMetricsEnabled;
    }


    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        if (legacyMetricsEnabled) {
            meterFactory.remove(BaseMetricName.AXON_QUERY, MeterFactory.SOURCE, event.getClientId());
            meterFactory.remove(BaseMetricName.AXON_QUERY, MeterFactory.TARGET, event.getClientId());
            meterFactory.remove(BaseMetricName.LOCAL_QUERY_RESPONSE_TIME, MeterFactory.TARGET, event.getClientId());
        }
        meterFactory.remove(BaseMetricName.QUERY_DURATION, MeterFactory.SOURCE, event.getClientId());
        meterFactory.remove(BaseMetricName.QUERY_DURATION, MeterFactory.TARGET, event.getClientId());
    }

    /**
     * Registers the duration of the handling of a query by a client.
     *
     * @param query          the name of the query
     * @param sourceClientId the source application requesting the query
     * @param targetClientId the unique id of the client application handling the query
     * @param context        the principal context application handling the query
     * @param duration       the duration
     */
    public void addHandlerResponseTime(QueryDefinition query,
                                       String sourceClientId,
                                       String targetClientId,
                                       String context,
                                       long duration) {
        if (legacyMetricsEnabled) {
            try {
                legacyTimer(query, sourceClientId, targetClientId, context).record(duration, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                logger.debug("Failed to create timer", ex);
            }
        }
    }

    /**
     * Retrieves the number of times that a query has been handled by a specific client.
     *
     * @param query          the definition of the query
     * @param targetClientId the client handling the query
     * @param context        the principal context of the client handling the query
     * @return cluster metric with access to the number of times the client has handled the query
     */
    public ClusterMetric clusterMetric(QueryDefinition query, String targetClientId, String context) {
        Tags tags = Tags.of(MeterFactory.CONTEXT, context,
                            MeterFactory.REQUEST, query.getQueryName(),
                            MeterFactory.TARGET, targetClientId);
        return new CompositeMetric(meterFactory.snapshot(BaseMetricName.QUERY_DURATION, tags),
                                   new Metrics(BaseMetricName.QUERY_DURATION.metric(),
                                               tags,
                                               meterFactory.clusterMetrics()));
    }


    private Timer legacyTimer(QueryDefinition query, String sourceClientId, String targetClientId, String context) {
        return meterFactory.timer(BaseMetricName.AXON_QUERY,
                                   Tags.of(
                                           MeterFactory.REQUEST, normalizeQueryName(query.getQueryName()),
                                           MeterFactory.CONTEXT, context,
                                           MeterFactory.SOURCE, sourceClientId,
                                           MeterFactory.TARGET, targetClientId));
    }

    private String normalizeQueryName(String query) {
        return query.replace(".", "/");
    }


    /**
     * Retrieves the number of times that a query has been handled by a specific client.
     *
     * @param query          the definition of the query
     * @param targetClientId the client handling the query
     * @param context        the principal context of the client handling the query
     * @param componentName  the client application name
     * @return QueryMetric containing the number of times that the query has been handled by this client
     */
    public QueryMetric queryMetric(QueryDefinition query, String targetClientId, String context, String componentName) {
        ClusterMetric clusterMetric = clusterMetric(query, targetClientId, context);
        return new QueryMetric(query, targetClientId, context, componentName, clusterMetric.count());
    }

    /**
     * Creates a gauge meter without any tags
     *
     * @param name          the name of the gauge
     * @param objectToWatch the object to watch
     * @param gaugeFunction function that will be applied on the object to retrieve the gauge value
     * @param <T>           type of object to watch
     * @return a gauge object
     */
    public <T> Gauge gauge(MetricName name, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return meterFactory.gauge(name, objectToWatch, gaugeFunction);
    }

    public Gauge gauge(MetricName name, Tags tags, Supplier<Number> gaugeFunction) {
        return meterFactory.gauge(name, tags, gaugeFunction);
    }

    /**
     * Creates a meter that will monitor the rate of certain events. The RateMeter will expose events/second for the
     * last 1/5/15 minutes and a
     * total count. The meter will have the context as a tag.
     *
     * @param context   the name of the context
     * @return a RateMeter object
     */
    public MeterFactory.RateMeter rateMeter(String context) {
        return meterFactory.rateMeter(BaseMetricName.QUERY_THROUGHPUT,
                                      legacyMetricsEnabled ? BaseMetricName.AXON_QUERY_RATE : null,
                                      Tags.of(MeterFactory.CONTEXT, context));
    }

    /**
     * Records the duration of the execution of a query on a specific handler.
     *
     * @param query            the identification of the query
     * @param clientId         the id of the client handling the query
     * @param context          the context of the query
     * @param durationInMillis duration of the query in milliseconds
     */
    public void addEndToEndResponseTime(QueryDefinition query, String clientId, String context, long durationInMillis) {
        if (legacyMetricsEnabled) {
            Tags tags = Tags.of(
                    MeterFactory.REQUEST, normalizeQueryName(query.getQueryName()),
                    MeterFactory.CONTEXT, context,
                    MeterFactory.TARGET, clientId);
            meterFactory.timer(BaseMetricName.LOCAL_QUERY_RESPONSE_TIME, tags).record(durationInMillis,
                                                                                      TimeUnit.MILLISECONDS);
        }
        meterFactory.timer(BaseMetricName.QUERY_DURATION, Tags.of(
                MeterFactory.REQUEST, query.getQueryName(),
                MeterFactory.CONTEXT, context,
                MeterFactory.TARGET, clientId)).record(durationInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Retrieves a snapshot of the response time metric, including percentile values over the last time window
     *
     * @param query    the identification of the query
     * @param clientId the id of the client handling the query
     * @param context  the context of the query
     * @return a snapshot for the response time metric
     */
    public HistogramSnapshot endToEndResponseTime(QueryDefinition query, String clientId, String context) {
        return meterFactory.timer(BaseMetricName.QUERY_DURATION, Tags.of(
                MeterFactory.REQUEST, query.getQueryName(),
                MeterFactory.CONTEXT, context,
                MeterFactory.TARGET, clientId)).takeSnapshot();
    }

    public void error(String command, String context, String errorCode) {
        meterFactory.counter(BaseMetricName.QUERY_ERRORS, Tags.of(MeterFactory.CONTEXT,
                                                                  context,
                                                                  MeterFactory.REQUEST,
                                                                  command,
                                                                  MeterFactory.ERROR_CODE,
                                                                  errorCode)).increment();
    }

    public static class QueryMetric {

        private final QueryDefinition queryDefinition;
        private final String clientId;
        private final String context;
        private final String componentName;
        private final long count;

        QueryMetric(QueryDefinition queryDefinition, String clientId, String context, String componentName, double count) {
            this.queryDefinition = queryDefinition;
            this.clientId = clientId;
            this.context = context;
            this.componentName = componentName;
            this.count = (long) count;
        }

        public QueryDefinition getQueryDefinition() {
            return queryDefinition;
        }

        public String getClientId() {
            return clientId;
        }

        public String getContext() {
            return context;
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
                    Objects.equals(clientId, that.clientId) &&
                    Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryDefinition, clientId, context);
        }
    }
}
