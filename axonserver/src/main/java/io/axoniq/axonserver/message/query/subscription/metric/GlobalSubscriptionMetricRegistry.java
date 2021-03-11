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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.UPDATE;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class GlobalSubscriptionMetricRegistry implements Supplier<SubscriptionMetrics> {

    private final Set<String> subscriptions = new CopyOnWriteArraySet<>();
    private final Counter total;
    private final BiFunction<String, Tags, ClusterMetric> clusterMetricCollector;

    private final AtomicInteger active = new AtomicInteger(0);
    private final Counter updates;

    public GlobalSubscriptionMetricRegistry(MeterFactory meterFactory,
                                            BiFunction<String, Tags, ClusterMetric> clusterMetricCollector) {

        this.updates = meterFactory.counter(BaseMetricName.AXON_GLOBAL_SUBSCRIPTION_UPDATES);
        this.total = meterFactory.counter(BaseMetricName.AXON_GLOBAL_SUBSCRIPTION_TOTAL);
        this.clusterMetricCollector = clusterMetricCollector;

        meterFactory.gauge(BaseMetricName.AXON_GLOBAL_SUBSCRIPTION_ACTIVE, active, AtomicInteger::get);
    }


    @Override
    public HubSubscriptionMetrics get() {
        return new HubSubscriptionMetrics(Tags.empty(),
                                          new GaugeMetric(BaseMetricName.AXON_GLOBAL_SUBSCRIPTION_ACTIVE.metric(),
                                                          () -> (double) active.get()),
                                          new CounterMetric(total.getId().getName(), () -> (long) total.count()),
                                          new CounterMetric(updates.getId().getName(), () -> (long) updates.count()),
                                          clusterMetricCollector);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryStarted event) {
        active.incrementAndGet();
        total.increment();
        subscriptions.add(event.subscriptionId());
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event){
        active.decrementAndGet();
        subscriptions.remove(event.subscriptionId());
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (subscriptions.contains(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            updates.increment();
        }
    }
}
