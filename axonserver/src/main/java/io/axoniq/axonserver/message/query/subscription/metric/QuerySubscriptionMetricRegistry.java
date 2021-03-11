/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription.metric;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CounterMetric;
import io.axoniq.axonserver.metric.GaugeMetric;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.UPDATE;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class QuerySubscriptionMetricRegistry  {

    private static final String TAG_QUERY = "query";

    private final MeterFactory localRegistry;
    private final BiFunction<String, Tags, ClusterMetric> clusterMetricProvider;
    private final Map<String, String> queries = new ConcurrentHashMap<>();
    private final Map<String, String> contexts = new ConcurrentHashMap<>();
    private final Map<String, Counter> totalSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, Counter> updatesMap = new ConcurrentHashMap<>();


    public QuerySubscriptionMetricRegistry(MeterFactory localRegistry,
                                           BiFunction<String, Tags, ClusterMetric> clusterMetricProvider) {
        this.localRegistry = localRegistry;
        this.clusterMetricProvider = clusterMetricProvider;
    }

    public HubSubscriptionMetrics get(String component, String query, String context) {
        AtomicInteger active = activeSubscriptionsMetric(query, context);
        Counter total = totalSubscriptionsMetric(query, context);
        Counter updates = updatesMetric(component,query, context);
        return new HubSubscriptionMetrics(
                Tags.of(MeterFactory.CONTEXT, context, TAG_QUERY, query),
                new GaugeMetric(BaseMetricName.AXON_QUERY_SUBSCRIPTION_ACTIVE.metric(), () -> (double) active.get()),
                new CounterMetric(total.getId().getName(), () -> (long) total.count()),
                new CounterMetric(updates.getId().getName(), () -> (long) updates.count()), clusterMetricProvider);
    }



    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryStarted event) {
        String queryName = event.subscription().getQueryRequest().getQuery();
        String context = event.context();
        activeSubscriptionsMetric(queryName, context).incrementAndGet();
        totalSubscriptionsMetric(queryName, context).increment();
        queries.put(event.subscriptionId(), queryName);
        contexts.put(event.subscriptionId(), context);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event) {
        String context = event.context();
        String queryName = event.unsubscribe().getQueryRequest().getQuery();
        contexts.remove(event.subscriptionId());
        if (queries.remove(event.subscriptionId(), queryName)) {
            activeSubscriptionsMetric(queryName, context).decrementAndGet();
        }
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (queries.containsKey(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            String componentName = event.response().getUpdate().getComponentName();
            String query = queries.get(event.subscriptionId());
            String context = contexts.get(event.subscriptionId());
            if (query != null && context != null) {
                updatesMetric(componentName, query, context).increment();
            }
        }
    }

    private AtomicInteger activeSubscriptionsMetric(String query, String context){

        return activeSubscriptionsMap.computeIfAbsent(activeSubscriptionsMetricName(query, context), name -> {
            AtomicInteger atomicInt = new AtomicInteger(0);
            localRegistry.gauge(BaseMetricName.AXON_QUERY_SUBSCRIPTION_ACTIVE,
                                Tags.of(MeterFactory.CONTEXT, context,
                                        TAG_QUERY, query),
                                atomicInt,
                                AtomicInteger::get);
            return atomicInt;
            }
        );
    }
    private String activeSubscriptionsMetricName(String query, String context) {
        return name(QuerySubscriptionMetricRegistry.class.getSimpleName(), query, context, "active");
    }

    private String name(String name, String query, String context, String active) {
        return String.format("axon.%s.%s.%s.%s", name, query, context, active);
    }
    private String name(String name, String component, String query, String context, String active) {
        return String.format("axon.%s.%s,%s.%s.%s", name, component, query, context, active);
    }

    private Counter totalSubscriptionsMetric(String query, String context){
        return totalSubscriptionsMap.computeIfAbsent(name(QuerySubscriptionMetricRegistry.class.getSimpleName(),
                                                          query,
                                                          context,
                                                          "total"),
                                                     name -> localRegistry
                                                             .counter(BaseMetricName.AXON_QUERY_SUBSCRIPTION_TOTAL,
                                                                      Tags.of(MeterFactory.CONTEXT,
                                                                              context,
                                                                              TAG_QUERY,
                                                                              query)));
    }

    private Counter updatesMetric(String component, String query, String context){
        return updatesMap.computeIfAbsent(name(QuerySubscriptionMetricRegistry.class.getSimpleName(),
                                               component,
                                               query,
                                               context,
                                               "updates"),
                                          name -> localRegistry.counter(BaseMetricName.AXON_QUERY_SUBSCRIPTION_UPDATES,
                                                                        Tags.of(MeterFactory.CONTEXT,
                                                                                context,
                                                                                TAG_QUERY,
                                                                                query)));
    }

}
