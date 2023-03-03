/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.transport.rest.PrincipalAuthentication;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Created by Sara Pellegrini on 18/04/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class QueryMetricsWebSocket {

    public static final String DESTINATION = "/topic/queries/";
    private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();

    private final QueryMetricsRegistry queryMetricsRegistry;

    private final QueryRegistrationCache queryRegistrationCache;

    private final Topology topology;
    private final SimpMessagingTemplate webSocket;

    public QueryMetricsWebSocket(QueryMetricsRegistry queryMetricsRegistry,
                                 QueryRegistrationCache queryRegistrationCache,
                                 Topology topology,
                                 SimpMessagingTemplate webSocket) {
        this.queryMetricsRegistry = queryMetricsRegistry;
        this.queryRegistrationCache = queryRegistrationCache;
        this.topology = topology;
        this.webSocket = webSocket;
    }

    @Scheduled(initialDelayString = "${axoniq.axonserver.websocket-update.initial-delay:10000}",
            fixedRateString = "${axoniq.axonserver.websocket-update.rate:1000}")
    public void publish() {
        if (subscriptions.isEmpty()) {
            return;
        }
        queryRegistrationCache.getAll().forEach(
                (queryDefinition, handlersPerComponent) -> getMetrics(queryDefinition, handlersPerComponent).forEach(
                        queryMetric -> subscriptions.forEach((destinations, contexts) -> {
                            if (contexts.contains(queryMetric.getContext())) {
                                webSocket.convertAndSend(destinations, queryMetric);
                            }
                        })
                ));
    }

    @EventListener
    public void on(SessionSubscribeEvent event) {
        StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
        String destination = sha.getDestination();
        if (destination != null && destination.startsWith(DESTINATION)) {
            subscriptions.put(destination, topology.visibleContexts(false, new PrincipalAuthentication(sha.getUser())));
        }
    }

    @EventListener
    public void on(SessionUnsubscribeEvent event) {
        StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
        if (sha.getDestination() != null) {
            subscriptions.remove(sha.getDestination());
        }
    }


    private Stream<QueryMetricsRegistry.QueryMetric> getMetrics(QueryDefinition queryDefinition,
                                                                Map<String, Set<QueryHandler<?>>> handlersPerComponents) {
        return handlersPerComponents
                .entrySet().stream()
                .flatMap(queryHandlers -> queryHandlers
                        .getValue()
                        .stream()
                        .map(queryHandler -> queryMetricsRegistry
                                .queryMetric(queryDefinition,
                                             queryHandler.getClientId(),
                                             queryHandler.getClientStreamIdentification().getContext(),
                                             queryHandlers.getKey())));
    }
}
