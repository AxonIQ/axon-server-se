/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.websocket;

import io.axoniq.axonserver.refactoring.messaging.command.CommandMetricsRegistry;
import io.axoniq.axonserver.refactoring.messaging.command.CommandRegistrationCache;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Stream;

/**
 * Created by Sara Pellegrini on 18/04/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class CommandMetricsWebSocket {

    public static final String DESTINATION = "/topic/commands";
    private final Set<SubscriptionKey> subscriptions = new CopyOnWriteArraySet<>();
    private final CommandMetricsRegistry commandMetricsRegistry;

    private final CommandRegistrationCache commandRegistrationCache;

    private final SimpMessagingTemplate webSocket;

    public CommandMetricsWebSocket(CommandMetricsRegistry commandMetricsRegistry,
                                   CommandRegistrationCache commandRegistrationCache,
                                   SimpMessagingTemplate webSocket) {
        this.commandMetricsRegistry = commandMetricsRegistry;
        this.commandRegistrationCache = commandRegistrationCache;
        this.webSocket = webSocket;
    }

    @Scheduled(initialDelayString = "${axoniq.axonserver.websocket-update.initial-delay:10000}",
            fixedRateString = "${axoniq.axonserver.websocket-update.rate:1000}")
    public void publish() {
        if (subscriptions.isEmpty()) {
            return;
        }
        commandRegistrationCache.getAll().forEach(
                (commandHandler, registrations) -> getMetrics(commandHandler, registrations).forEach(
                        commandMetric -> webSocket.convertAndSend(DESTINATION, commandMetric)
                ));
    }

    @EventListener
    public void on(SessionSubscribeEvent event) {
        StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
        if (DESTINATION.equals(sha.getDestination())) {
            subscriptions.add(new SubscriptionKey(sha));
        }
    }

    @EventListener
    public void on(SessionUnsubscribeEvent event) {
        StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
        subscriptions.remove(new SubscriptionKey(sha));
    }

    private Stream<CommandMetricsRegistry.CommandMetric> getMetrics(CommandHandler commandHander,
                                                                    Set<CommandRegistrationCache.RegistrationEntry> registrations) {
        return registrations.stream()
                            .map(registration -> commandMetricsRegistry
                                    .commandMetric(registration.getCommand(),
                                                   commandHander.client().id(),
                                                   commandHander.context(),
                                                   commandHander.client().applicationName()));
    }
}
