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
import io.axoniq.axonserver.serializer.Media;

import java.util.function.Function;

/**
 * Created by Sara Pellegrini on 19/06/2018.
 * sara.pellegrini@gmail.com
 */
public class HubSubscriptionMetrics implements SubscriptionMetrics {

    private final ClusterMetric totalSubscriptions;
    private final ClusterMetric activeSubscriptions;
    private final ClusterMetric updates;

    public HubSubscriptionMetrics(CounterMetric active, CounterMetric total, CounterMetric updates,
                                  Function<String, ClusterMetric> clusterRegistry) {
        this(
                new CompositeMetric(total, clusterRegistry.apply(total.getName())),
                new CompositeMetric(active, clusterRegistry.apply(active.getName())),
                new CompositeMetric(updates, clusterRegistry.apply(updates.getName()))
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
        return totalSubscriptions.size();
    }

    @Override
    public Long activesCount() {
        return activeSubscriptions.size();
    }

    @Override
    public Long updatesCount() {
        return updates.size();
    }


    @Override
    public void printOn(Media media) {
        media.with("totalSubscriptions", totalCount());
        media.with("activeSubscriptions", activesCount());
        media.with("updates", updatesCount());
    }
}
