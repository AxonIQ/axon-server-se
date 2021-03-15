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
public class ApplicationSubscriptionMetricRegistry {

    private static final String TAG_COMPONENT = "component";
    private final MeterFactory localMetricRegistry;
    private final BiFunction<String, Tags, ClusterMetric> clusterMetricProvider;
    private final Map<String, String> componentNames = new ConcurrentHashMap<>();
    private final Map<String, String> contexts = new ConcurrentHashMap<>();
    private final Map<String, Counter> totalSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, Counter> updatesMap = new ConcurrentHashMap<>();

    public ApplicationSubscriptionMetricRegistry(MeterFactory localMetricRegistry,
                                                 BiFunction<String, Tags, ClusterMetric> clusterMetricProvider) {
        this.localMetricRegistry = localMetricRegistry;
        this.clusterMetricProvider = clusterMetricProvider;
    }

    public HubSubscriptionMetrics get(String componentName, String context) {
        AtomicInteger active = activeSubscriptionsMetric(componentName, context);
        Counter total = totalSubscriptionsMetric(componentName, context);
        Counter updates = updatesMetric(componentName, context);
        return new HubSubscriptionMetrics(
                Tags.of(MeterFactory.CONTEXT, context, TAG_COMPONENT, componentName),
                new GaugeMetric(BaseMetricName.AXON_APPLICATION_SUBSCRIPTION_ACTIVE.metric(),
                                () -> (double) active.get()),
                new CounterMetric(total.getId().getName(), () -> (long) total.count()),
                new CounterMetric(updates.getId().getName(), () -> (long) updates.count()),
                clusterMetricProvider);
    }



    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryStarted event) {
        String componentName = event.subscription().getQueryRequest().getComponentName();
        String context = event.context();
        activeSubscriptionsMetric(componentName, context).incrementAndGet();
        totalSubscriptionsMetric(componentName, context).increment();
        componentNames.putIfAbsent(event.subscriptionId(), componentName);
        contexts.putIfAbsent(event.subscriptionId(), context);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event) {
        String componentName = event.unsubscribe().getQueryRequest().getComponentName();
        String context = event.context();
        componentNames.remove(event.subscriptionId());
        if (contexts.remove(event.subscriptionId()) != null) {
            activeSubscriptionsMetric(componentName, context).decrementAndGet();
        }
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (componentNames.containsKey(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            String component = componentNames.get(event.subscriptionId());
            String context = contexts.get(event.subscriptionId());
            if (component != null && context != null) {
                updatesMetric(component, context).increment();
            }
        }
    }

    private AtomicInteger activeSubscriptionsMetric(String component, String context){
        return activeSubscriptionsMap.computeIfAbsent(activeSubscriptionsMetricName(component, context), name -> {
            AtomicInteger atomicInteger = new AtomicInteger(0);
            localMetricRegistry.gauge(BaseMetricName.AXON_APPLICATION_SUBSCRIPTION_ACTIVE, Tags.of(MeterFactory.CONTEXT,
                                                                                                   context,
                                                                                                   TAG_COMPONENT,
                                                                                                   component),
                                      atomicInteger, AtomicInteger::get);
            return atomicInteger;
        });
    }

    private String activeSubscriptionsMetricName(String component, String context) {
        return name(ApplicationSubscriptionMetricRegistry.class.getSimpleName(), component, context, "active");
    }

    private String name(String name, String component, String context, String active) {
        return String.format("axon.%s.%s.%s.%s", name, component, context, active);
    }

    private Counter totalSubscriptionsMetric(String component, String context){
        return totalSubscriptionsMap.computeIfAbsent(name(ApplicationSubscriptionMetricRegistry.class.getSimpleName(), component, context, "total"),
                                                     name -> localMetricRegistry
                                                             .counter(BaseMetricName.AXON_APPLICATION_SUBSCRIPTION_TOTAL,
                                                                      Tags.of(MeterFactory.CONTEXT, context,
                                                                              TAG_COMPONENT, component)));

    }

    private Counter updatesMetric(String component, String context){
        return updatesMap.computeIfAbsent(name(ApplicationSubscriptionMetricRegistry.class.getSimpleName(),
                                               component,
                                               context,
                                               "updates"),
                                          name -> localMetricRegistry
                                                  .counter(BaseMetricName.AXON_APPLICATION_SUBSCRIPTION_UPDATES,
                                                           Tags.of(MeterFactory.CONTEXT,
                                                                   context,
                                                                   TAG_COMPONENT,
                                                                   component)));
    }
}
