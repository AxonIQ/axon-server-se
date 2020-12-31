/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.ProxiedSubscriptionQueryRequest;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryRequested;
import io.axoniq.axonserver.applicationevents.TopologyEvents.QueryHandlerDisconnected;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.message.query.subscription.DirectSubscriptionQueries.ContextSubscriptionQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by Sara Pellegrini on 04/05/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class SubscriptionQueryDispatcher {

    private final Logger logger = LoggerFactory.getLogger(SubscriptionQueryDispatcher.class);
    private final Iterable<ContextSubscriptionQuery> directSubscriptions;
    private final QueryRegistrationCache registrationCache;
    private final Map<ClientStreamIdentification, Set<String>> subscriptionsSent = new ConcurrentHashMap<>();

    public SubscriptionQueryDispatcher(Iterable<ContextSubscriptionQuery> directSubscriptions,
                                       QueryRegistrationCache registrationCache) {
        this.directSubscriptions = directSubscriptions;
        this.registrationCache = registrationCache;
    }

    @EventListener
    public void on(ProxiedSubscriptionQueryRequest event) {
        SubscriptionQueryRequest request = event.subscriptionQueryRequest();
        SubscriptionQuery query = event.subscriptionQuery();
        QueryHandler handler = registrationCache.find(event.context(), query.getQueryRequest(), event.targetClient());
        handler.dispatch(request);
    }


    @EventListener
    public void on(SubscriptionQueryRequested event) {
        logger.debug("Dispatch subscription query request with subscriptionId = {}", event.subscriptionId());
        SubscriptionQuery query = event.subscription();
        Collection<? extends QueryHandler> handlers = registrationCache.findAll(event.context(),
                                                                                query.getQueryRequest());
        if (handlers == null || handlers.isEmpty()) {
            event.errorHandler().accept(new IllegalArgumentException(ErrorCode.NO_HANDLER_FOR_QUERY.getCode()));
            return;
        }
        handlers.forEach(handler -> {
            handler.dispatch(event.subscriptionQueryRequest());
            subscriptionsSent.computeIfAbsent(handler.getClientStreamIdentification(),
                                              client -> new CopyOnWriteArraySet<>()).add(event.subscriptionId());
        });
    }

    @EventListener
    public void on(SubscriptionQueryInitialResultRequested event) {
        String subscriptionId = event.subscriptionId();
        logger.debug("Dispatch initial result request for subscription query with subscriptionId = {}", subscriptionId);
        SubscriptionQuery query = event.subscription();
        Collection<? extends QueryHandler> handlers = registrationCache.find(event.context(), query.getQueryRequest());
        if (handlers == null || handlers.isEmpty()) {
            event.errorHandler().accept(new IllegalArgumentException(ErrorCode.NO_HANDLER_FOR_QUERY.getCode()));
            return;
        }
        handlers.forEach(handler -> {
            handler.dispatch(event.subscriptionQueryRequest());
            subscriptionsSent.computeIfAbsent(handler.getClientStreamIdentification(),
                                              client -> new CopyOnWriteArraySet<>()).add(event.subscriptionId());
        });
    }

    @EventListener
    public void on(SubscriptionQueryCanceled evt) {
        logger.debug("Dispatch subscription query cancel with subscriptionId = {}", evt.subscriptionId());
        SubscriptionQueryRequest queryRequest = SubscriptionQueryRequest.newBuilder()
                                                                        .setUnsubscribe(evt.unsubscribe())
                                                                        .build();
        Collection<QueryHandler> handlers = registrationCache.findAll(evt.context(),
                                                                      evt.unsubscribe().getQueryRequest());
        handlers.forEach(handler -> safeDispatch(handler, queryRequest));
    }

    private void safeDispatch(QueryHandler handler,
                              SubscriptionQueryRequest queryRequest) {
        try {
            handler.dispatch(queryRequest);
        } catch (Exception ex) {
            logger.debug("Dispatch subscription query cancel with subscriptionId = {} failed",
                         queryRequest.getUnsubscribe().getSubscriptionIdentifier(),
                         ex);
        }
    }


    @EventListener
    public void on(SubscriptionEvents.SubscribeQuery event) {
        ClientStreamIdentification clientStreamIdentification = event.clientIdentification();
        QueryDefinition queryDefinition = new QueryDefinition(event.getContext(), event.getSubscription().getQuery());
        directSubscriptions.forEach(subscription -> {
            String subscriptionId = subscription.subscriptionQuery().getSubscriptionIdentifier();
            QueryDefinition query = new QueryDefinition(subscription.context(), subscription.queryName());
            Set<String> ids = subscriptionsSent.computeIfAbsent(clientStreamIdentification,
                                                                client -> new CopyOnWriteArraySet<>());
            if (queryDefinition.equals(query) && !ids.contains(subscriptionId)) {
                SubscriptionQuery subscriptionQuery = subscription.subscriptionQuery();
                event.getQueryHandler().dispatch(SubscriptionQueryRequest.newBuilder()
                                                                         .setSubscribe(subscriptionQuery)
                                                                         .build());
                ids.add(subscriptionId);
            }
        });
    }

    @EventListener
    public void on(QueryHandlerDisconnected event) {
        subscriptionsSent.remove(new ClientStreamIdentification(event.getContext(), event.getClientStreamId()));
    }
}
