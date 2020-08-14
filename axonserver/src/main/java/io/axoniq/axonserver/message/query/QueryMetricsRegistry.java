/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.Metrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
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
 * Registry for metrics regarding query execution.
 * @author Marc Gathier
 * @since 4.0
 */
@Service("QueryMetricsRegistry")
public class QueryMetricsRegistry {
    private final Logger logger = LoggerFactory.getLogger(QueryMetricsRegistry.class);
    private final Map<String, Timer> timerMap = new ConcurrentHashMap<>();
    private final MeterFactory meterFactory;

    /**
     * Constructor of the registry.
     *
     * @param meterFactory factory to create meters.
     */
    public QueryMetricsRegistry(MeterFactory meterFactory) {
        this.meterFactory = meterFactory;
    }

    /**
     * Registers the duration of the handling of a query by a client.
     *
     * @param query          the name of the query
     * @param sourceClientId the source application requesting the query
     * @param clientId       the application handling the query
     * @param duration       the duration
     */
    //TODO
    public void add(QueryDefinition query, String sourceClientId, ClientStreamIdentification clientId, long duration) {
        try {
            timer(query, sourceClientId, clientId).record(duration, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            logger.debug("Failed to create timer", ex);
        }
    }

    /**
     * Retrieves the number of times that a query has been handled by a specific client.
     *
     * @param query    the definition of the query
     * @param clientId the client handling the query
     * @return cluster metric with access to the number of times the client has handled the query
     */
    public ClusterMetric clusterMetric(QueryDefinition query, ClientStreamIdentification clientId) {
        Tags tags = Tags.of(MeterFactory.CONTEXT, clientId.getContext(),
                            MeterFactory.REQUEST, query.getQueryName().replaceAll("\\.", "/"),
                            MeterFactory.TARGET, clientId.getClientStreamId());
        return new CompositeMetric(meterFactory.snapshot(BaseMetricName.AXON_QUERY, tags),
                                   new Metrics(BaseMetricName.AXON_QUERY.metric(),
                                               tags,
                                               meterFactory.clusterMetrics()));
    }


    private Timer timer(QueryDefinition query, String sourceClientId, ClientStreamIdentification clientId) {
        String metricName = metricName(query, sourceClientId, clientId);
        return timerMap.computeIfAbsent(metricName, n ->
                meterFactory.timer(BaseMetricName.AXON_QUERY,
                                   Tags.of(
                                           MeterFactory.REQUEST, query.getQueryName().replaceAll("\\.", "/"),
                                           MeterFactory.CONTEXT, clientId.getContext(),
                                           MeterFactory.SOURCE, sourceClientId,
                                           MeterFactory.TARGET, clientId.getClientStreamId())));
    }

    private String metricName(QueryDefinition query, String sourceClientId, ClientStreamIdentification clientId) {
        return String.format("%s.%s.%s", query.getQueryName(), sourceClientId, clientId.metricName());
    }

    /**
     * Retrieves the number of times that a query has been handled by a specific client.
     *
     * @param query         the definition of the query
     * @param clientId      the client handling the query
     * @param componentName the client application name
     * @return QueryMetric containing the number of times that the query has been handled by this client
     */
    public QueryMetric queryMetric(QueryDefinition query, ClientStreamIdentification clientId, String componentName) {
        ClusterMetric clusterMetric = clusterMetric(query, clientId);
        return new QueryMetric(query, clientId.metricName(), componentName, clusterMetric.count());
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
    public <T> Gauge gauge(BaseMetricName name, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return meterFactory.gauge(name, objectToWatch, gaugeFunction);
    }

    /**
     * Creates a meter that will monitor the rate of certain events. The RateMeter will expose events/second for the
     * last 1/5/15 minutes and a
     * total count. The meter will have the context as a tag.
     *
     * @param context   the name of the context
     * @param meterName the name of the meter
     * @return a RateMeter object
     */
    public MeterFactory.RateMeter rateMeter(String context, BaseMetricName meterName) {
        return meterFactory.rateMeter(meterName, Tags.of(MeterFactory.CONTEXT, context));
    }


    public static class QueryMetric {
        private final QueryDefinition queryDefinition;
        private final String clientId;
        private final String componentName;
        private final long count;

        QueryMetric(QueryDefinition queryDefinition, String clientId, String componentName, double count) {
            this.queryDefinition = queryDefinition;
            this.clientId = clientId;
            this.componentName = componentName;
            this.count = (long) count;
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
