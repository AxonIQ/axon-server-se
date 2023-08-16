/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription.metric;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.INITIAL_RESULT;
import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.UPDATE;
import static io.axoniq.axonserver.metric.BaseMetricName.AXON_SUBSCRIPTION_ACTIVE;
import static io.axoniq.axonserver.metric.BaseMetricName.AXON_SUBSCRIPTION_TOTAL;
import static io.axoniq.axonserver.metric.BaseMetricName.AXON_SUBSCRIPTION_UPDATES;
import static io.axoniq.axonserver.metric.MeterFactory.CONTEXT;
import static io.axoniq.axonserver.metric.MeterFactory.REQUEST;
import static io.axoniq.axonserver.metric.MeterFactory.SOURCE;
import static io.axoniq.axonserver.metric.MeterFactory.TARGET;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class SubscriptionQueryMetricRegistry {

    private static final String TAG_COMPONENT = "component";
    private final MeterFactory localMetricRegistry;
    private final BiFunction<String, Tags, ClusterMetric> clusterMetricProvider;
    private final Map<String, String> componentNames = new ConcurrentHashMap<>();
    private final Map<String, String> clients = new ConcurrentHashMap<>();
    private final Map<String, Long> started = new ConcurrentHashMap<>();
    private final Map<String, String> contexts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, String> queries = new ConcurrentHashMap<>();

    public SubscriptionQueryMetricRegistry(MeterFactory localMetricRegistry,
                                           BiFunction<String, Tags, ClusterMetric> clusterMetricProvider) {
        this.localMetricRegistry = localMetricRegistry;
        this.clusterMetricProvider = clusterMetricProvider;
    }

    public HubSubscriptionMetrics getByComponentAndContext(String componentName, String context) {
        Tags tags = Tags.of(MeterFactory.CONTEXT, context,
                            TAG_COMPONENT, componentName);
        return getByTags(tags);
    }

    public SubscriptionMetrics getByComponentAndQuery(String componentName, String queryName, String context) {
        Tags tags = Tags.of(MeterFactory.CONTEXT, context, REQUEST, queryName,
                            TAG_COMPONENT, componentName);
        return getByTags(tags);
    }

    private HubSubscriptionMetrics getByTags(Tags tags) {
        return new HubSubscriptionMetrics(
                tags,
                new GaugeMetric(AXON_SUBSCRIPTION_ACTIVE.metric(),
                                () -> localMetricRegistry.sum(AXON_SUBSCRIPTION_ACTIVE,
                                                              tags)),
                new CounterMetric(AXON_SUBSCRIPTION_TOTAL.metric(), () -> localMetricRegistry.count(
                        AXON_SUBSCRIPTION_TOTAL, tags)),
                new CounterMetric(AXON_SUBSCRIPTION_UPDATES.metric(), () -> localMetricRegistry.count(
                        AXON_SUBSCRIPTION_UPDATES, tags)),
                clusterMetricProvider);
    }

    public HubSubscriptionMetrics getByRequestAndContext(String request, String context) {
        Tags tags = Tags.of(MeterFactory.CONTEXT, context,
                            REQUEST, request);
        return getByTags(tags);
    }

    public HubSubscriptionMetrics get(String context) {
        return getByTags(Tags.of(MeterFactory.CONTEXT, context));
    }


    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryStarted event) {
        String componentName = event.subscription().getQueryRequest().getComponentName();
        String context = event.context();
        String request = event.subscription().getQueryRequest().getQuery();
        String clientId = event.subscription().getQueryRequest().getClientId();
        activeSubscriptionsMetric(componentName, context, request).incrementAndGet();
        totalSubscriptionsMetric(componentName, context, request).increment();
        contexts.putIfAbsent(event.subscriptionId(), context);
        componentNames.putIfAbsent(event.subscriptionId(), componentName);
        queries.putIfAbsent(event.subscriptionId(), request);
        clients.putIfAbsent(event.subscriptionId(), clientId);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested event) {
        String client = clients.get(event.subscriptionId());
        if (client != null) {
            started.putIfAbsent(event.subscriptionId(), System.currentTimeMillis());
        }
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event) {
        String componentName = event.unsubscribe().getQueryRequest().getComponentName();
        String context = event.context();
        String request = event.unsubscribe().getQueryRequest().getQuery();
        componentNames.remove(event.subscriptionId());
        queries.remove(event.subscriptionId());
        clients.remove(event.subscriptionId());
        started.remove(event.subscriptionId());
        if (contexts.remove(event.subscriptionId()) != null) {
            activeSubscriptionsMetric(componentName, context, request).decrementAndGet();
        }
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event) {
        String subscriptionId = event.subscriptionId();
        Long startTime = started.remove(subscriptionId);
        if (startTime != null && event.response().getResponseCase().equals(INITIAL_RESULT)
                && event.clientId() != null) {
            String clientId = clients.getOrDefault(subscriptionId, "UNKNOWN_CLIENT");
            String context = contexts.getOrDefault(subscriptionId, "UNKNOWN_CONTEXT");
            String request = queries.getOrDefault(subscriptionId, "UNKNOWN_REQUEST");
            long durationInMillis = System.currentTimeMillis() - startTime;
            localMetricRegistry.timer(BaseMetricName.AXON_QUERY,
                                      Tags.of(CONTEXT,
                                              context,
                                              REQUEST,
                                              request.replace(".", "/"),
                                              SOURCE,
                                              clientId,
                                              TARGET,
                                              event.clientId()))
                               .record(durationInMillis, TimeUnit.MILLISECONDS);
            localMetricRegistry.timer(BaseMetricName.LOCAL_QUERY_RESPONSE_TIME, Tags.of(
                    MeterFactory.REQUEST, request.replace(".", "/"),
                    MeterFactory.CONTEXT, context,
                    MeterFactory.TARGET, clientId)).record(durationInMillis, TimeUnit.MILLISECONDS);
        }

        if (componentNames.containsKey(subscriptionId) && event.response().getResponseCase().equals(UPDATE)) {
            String component = componentNames.get(subscriptionId);
            String context = contexts.get(subscriptionId);
            String request = queries.get(subscriptionId);

            if (component != null && context != null) {
                updatesMetric(component, context, request).increment();
            }
        }
    }

    private AtomicInteger activeSubscriptionsMetric(String component, String context, String request) {
        return activeSubscriptionsMap.computeIfAbsent(activeSubscriptionsMetricName(component, context, request),
                                                      name -> {
                                                          AtomicInteger atomicInteger = new AtomicInteger(0);
                                                          localMetricRegistry.gauge(AXON_SUBSCRIPTION_ACTIVE,
                                                                                    Tags.of(MeterFactory.CONTEXT,
                                                                                            context,
                                                                                            TAG_COMPONENT,
                                                                                            component,
                                                                                            REQUEST,
                                                                                            request),
                                                                                    atomicInteger,
                                                                                    AtomicInteger::get);
                                                          return atomicInteger;
                                                      });
    }

    private String activeSubscriptionsMetricName(String component, String context, String request) {
        return name(component, context, request);
    }

    private String name(String component, String context, String request) {
        return String.format("%s.%s.%s", component, context, request);
    }

    private Counter totalSubscriptionsMetric(String component, String context, String request) {
        return localMetricRegistry
                .counter(AXON_SUBSCRIPTION_TOTAL,
                         Tags.of(MeterFactory.CONTEXT, context,
                                 TAG_COMPONENT, component,
                                 REQUEST, request));
    }

    private Counter updatesMetric(String component, String context, String request) {
        return localMetricRegistry
                .counter(AXON_SUBSCRIPTION_UPDATES,
                         Tags.of(MeterFactory.CONTEXT,
                                 context,
                                 TAG_COMPONENT,
                                 component,
                                 REQUEST, request));
    }
}
