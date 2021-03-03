/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription.metric;

import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.CounterMetric;
import io.axoniq.axonserver.metric.GaugeMetric;
import io.axoniq.axonserver.serializer.Media;
import io.micrometer.core.instrument.Tags;

import java.util.function.BiFunction;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class HubSubscriptionMetrics implements SubscriptionMetrics {

    private final ClusterMetric totalSubscriptions;
    private final ClusterMetric activeSubscriptions;
    private final ClusterMetric updates;

    public HubSubscriptionMetrics(Tags tags, GaugeMetric active, CounterMetric total, CounterMetric updates,
                                  BiFunction<String, Tags, ClusterMetric> clusterRegistry) {
        this(
                new CompositeMetric(total, clusterRegistry.apply(total.getName(), tags)),
                new CompositeMetric(active, clusterRegistry.apply(active.getName(), tags)),
                new CompositeMetric(updates, clusterRegistry.apply(updates.getName(), tags))
        );
    }

    public HubSubscriptionMetrics(ClusterMetric totalSubscriptions,
                                  ClusterMetric activeSubscriptions, ClusterMetric updates) {
        this.totalSubscriptions = totalSubscriptions;
        this.activeSubscriptions = activeSubscriptions;
        this.updates = updates;
    }


    @Override
    public Long totalCount() {
        return totalSubscriptions.count();
    }

    @Override
    public Long activesCount() {
        return (long) activeSubscriptions.value();
    }

    @Override
    public Long updatesCount() {
        return updates.count();
    }


    @Override
    public void printOn(Media media) {
        media.with("totalSubscriptions", totalCount());
        media.with("activeSubscriptions", activesCount());
        media.with("updates", updatesCount());
    }
}
