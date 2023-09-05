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
import io.micrometer.core.instrument.Timer;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.INITIAL_RESULT;
import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.UPDATE;
import static io.axoniq.axonserver.metric.BaseMetricName.AXON_SUBSCRIPTION_ACTIVE;
import static io.axoniq.axonserver.metric.BaseMetricName.AXON_SUBSCRIPTION_DURATION;
import static io.axoniq.axonserver.metric.BaseMetricName.AXON_SUBSCRIPTION_TOTAL;
import static io.axoniq.axonserver.metric.BaseMetricName.AXON_SUBSCRIPTION_UPDATES;
import static io.axoniq.axonserver.metric.MeterFactory.CONTEXT;
import static io.axoniq.axonserver.metric.MeterFactory.REQUEST;
import static io.axoniq.axonserver.metric.MeterFactory.SOURCE;
import static io.axoniq.axonserver.metric.MeterFactory.TARGET;

/**
 * Component capturing metrics for subscription queries.
 *
 * @author Marc Gathier
 * @since 2023.2.0
 */
@Component
public class SubscriptionQueryMetricRegistry {

    public static final String TAG_COMPONENT = "component";
    private final MeterFactory localMetricRegistry;
    private final BiFunction<String, Tags, ClusterMetric> clusterMetricProvider;
    private final Clock clock;
    private final Map<String, AtomicInteger> activeSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, ActiveSubscriptionQuery> activeSubscriptionQueryMap = new ConcurrentHashMap<>();
    private final Map<String, Long> initialQueryStartedMap = new ConcurrentHashMap<>();

    public SubscriptionQueryMetricRegistry(MeterFactory localMetricRegistry,
                                           BiFunction<String, Tags, ClusterMetric> clusterMetricProvider,
                                           Clock clock) {
        this.localMetricRegistry = localMetricRegistry;
        this.clusterMetricProvider = clusterMetricProvider;
        this.clock = clock;
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
        activeSubscriptionsMetric(componentName, context, request).incrementAndGet();
        totalSubscriptionsMetric(componentName, context, request).increment();
        activeSubscriptionQueryMap.putIfAbsent(event.subscriptionId(),
                                               new ActiveSubscriptionQuery(componentName, context, request));
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested event) {
        initialQueryStartedMap.put(event.subscriptionId(), clock.millis());
    }
    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event) {
        ActiveSubscriptionQuery activeSubscriptionQuery = activeSubscriptionQueryMap.remove(event.subscriptionId());
        if (activeSubscriptionQuery != null) {
            activeSubscriptionsMetric(activeSubscriptionQuery.componentName,
                                      activeSubscriptionQuery.context,
                                      activeSubscriptionQuery.request).decrementAndGet();

            subscriptionDurationMetric(activeSubscriptionQuery.componentName,
                                       activeSubscriptionQuery.context,
                                       activeSubscriptionQuery.request)
                    .record(clock.millis() - activeSubscriptionQuery.started, TimeUnit.MILLISECONDS);
        }
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event) {
        ActiveSubscriptionQuery activeSubscriptionQuery = activeSubscriptionQueryMap.get(event.subscriptionId());
        if (activeSubscriptionQuery != null && event.response().getResponseCase().equals(INITIAL_RESULT)
                && event.clientId() != null) {
            Long initialQueryStarted = initialQueryStartedMap.remove(event.subscriptionId());
            if (initialQueryStarted != null) {
                String clientId = event.clientId();
                long durationInMillis = clock.millis() - initialQueryStarted;
                localMetricRegistry.timer(BaseMetricName.AXON_QUERY,
                                          Tags.of(CONTEXT,
                                                  activeSubscriptionQuery.context,
                                                  REQUEST,
                                                  normalizeRequest(activeSubscriptionQuery.request),
                                                  SOURCE,
                                                  clientId,
                                                  TARGET,
                                                  event.clientId()))
                                   .record(durationInMillis, TimeUnit.MILLISECONDS);
                localMetricRegistry.timer(BaseMetricName.LOCAL_QUERY_RESPONSE_TIME, Tags.of(
                        MeterFactory.REQUEST, normalizeRequest(activeSubscriptionQuery.request),
                        MeterFactory.CONTEXT, activeSubscriptionQuery.context,
                        MeterFactory.TARGET, clientId)).record(durationInMillis,
                                                               TimeUnit.MILLISECONDS);
            }
        }

        if (activeSubscriptionQuery != null && event.response().getResponseCase().equals(UPDATE)) {
            String component = activeSubscriptionQuery.componentName;
            String context = activeSubscriptionQuery.context;
            String request = activeSubscriptionQuery.request;

            if (component != null && context != null) {
                updatesMetric(component, context, request).increment();
            }
        }
    }

    private String normalizeRequest(String request) {
        return request.replace(".", "/");
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

    private Timer subscriptionDurationMetric(String component, String context, String request) {
        return localMetricRegistry
                .timer(AXON_SUBSCRIPTION_DURATION,
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

    private class ActiveSubscriptionQuery {

        final long started = clock.millis();
        final String componentName;
        final String context;
        final String request;

        private ActiveSubscriptionQuery(String componentName, String context, String request) {
            this.componentName = componentName;
            this.context = context;
            this.request = request;
        }
    }
}
