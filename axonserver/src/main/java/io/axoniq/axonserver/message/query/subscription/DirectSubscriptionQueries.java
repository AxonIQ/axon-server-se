/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryStarted;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * {@link Iterable} of active {@link ContextSubscriptionQuery}s from clients that are directly connected to the AS node.
 *
 * @author Sara Pellegrini
 */
@Component
public class DirectSubscriptionQueries implements Iterable<DirectSubscriptionQueries.ContextSubscriptionQuery> {

    //Map<clientId, Map<subscriptionId, ContextSubscriptionQuery>>
    private final Map<String, Map<String, ContextSubscriptionQuery>> map = new ConcurrentHashMap<>();

    @EventListener
    public void on(SubscriptionQueryStarted event) {
        SubscriptionQuery request = event.subscription();
        QueryRequest query = request.getQueryRequest();
        ContextSubscriptionQuery contextSubscriptionQuery = new ContextSubscriptionQuery(event.context(), request);
        clientRequests(query.getClientId()).put(request.getSubscriptionIdentifier(), contextSubscriptionQuery);
    }

    @EventListener
    public void on(SubscriptionQueryCanceled event) {
        QueryRequest request = event.unsubscribe().getQueryRequest();
        clientRequests(request.getClientId()).remove(event.subscriptionId());
    }

    @Override
    @Nonnull
    public Iterator<ContextSubscriptionQuery> iterator() {
        return map.values().stream().map(Map::values).flatMap(Collection::stream).iterator();
    }

    private Map<String, ContextSubscriptionQuery> clientRequests(String clientId) {
        return map.computeIfAbsent(clientId, id -> new ConcurrentHashMap<>());
    }

    public static class ContextSubscriptionQuery {

        private final String context;

        private final SubscriptionQuery subscriptionQuery;

        ContextSubscriptionQuery(String context, SubscriptionQuery subscriptionQuery) {
            this.context = context;
            this.subscriptionQuery = subscriptionQuery;
        }

        public String context() {
            return context;
        }

        SubscriptionQuery subscriptionQuery() {
            return subscriptionQuery;
        }

        String queryName(){
            return subscriptionQuery.getQueryRequest().getQuery();
        }
    }
}
