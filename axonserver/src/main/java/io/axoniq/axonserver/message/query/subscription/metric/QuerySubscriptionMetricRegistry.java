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
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CounterMetric;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.UPDATE;

/**
 * Created by Sara Pellegrini on 20/06/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class QuerySubscriptionMetricRegistry  {
    private final MeterRegistry localRegistry;
    private final Function<String, ClusterMetric> clusterRegistry;
    private final Map<String, String> queries = new ConcurrentHashMap<>();
    private final Map<String, String> contexts = new ConcurrentHashMap<>();
    private final Map<String, Counter> totalSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, Counter> updatesMap = new ConcurrentHashMap<>();


    public QuerySubscriptionMetricRegistry(MeterRegistry localRegistry,
                                                 Function<String, ClusterMetric> clusterRegistry) {
        this.localRegistry = localRegistry;
        this.clusterRegistry = clusterRegistry;
    }

    public HubSubscriptionMetrics get(String component, String query, String context) {
        AtomicInteger active = activeSubscriptionsMetric(query, context);
        Counter total = totalSubscriptionsMetric(query, context);
        Counter updates = updatesMetric(component,query, context);
        return new HubSubscriptionMetrics(new CounterMetric(activeSubscriptionsMetricName(query, context), ()-> (long)active.get()),
                                          new CounterMetric(total.getId().getName(), () -> (long)total.count()),
                                          new CounterMetric(updates.getId().getName(), () -> (long)updates.count()), clusterRegistry);
    }



    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryRequested event){
        String queryName = event.subscription().getQueryRequest().getQuery();
        String context = event.context();
        activeSubscriptionsMetric(queryName, context).incrementAndGet();
        totalSubscriptionsMetric(queryName, context).increment();
        queries.put(event.subscriptionId(), queryName);
        contexts.put(event.subscriptionId(), context);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event){
        String queryName = event.unsubscribe().getQueryRequest().getQuery();
        String context = contexts.remove(event.subscriptionId());
        activeSubscriptionsMetric(queryName, context).decrementAndGet();
        queries.remove(event.subscriptionId(), queryName);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (queries.containsKey(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            String componentName = event.response().getUpdate().getComponentName();
            String query = queries.get(event.subscriptionId());
            String context = contexts.get(event.subscriptionId());
            updatesMetric(componentName, query, context).increment();
        }
    }

    private AtomicInteger activeSubscriptionsMetric(String query, String context){

        return activeSubscriptionsMap.computeIfAbsent(activeSubscriptionsMetricName(query, context), name -> {
            AtomicInteger atomicInt = new AtomicInteger(0);
            Gauge.builder(name, atomicInt, AtomicInteger::get).register(localRegistry);
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
        return totalSubscriptionsMap.computeIfAbsent(name(QuerySubscriptionMetricRegistry.class.getSimpleName(), query, context, "total"), name -> localRegistry.counter(name));
    }

    private Counter updatesMetric(String component, String query, String context){
        return updatesMap.computeIfAbsent(name(QuerySubscriptionMetricRegistry.class.getSimpleName(), component, query, context, "updates"), name -> localRegistry.counter(name));
    }

}
