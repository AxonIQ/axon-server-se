/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerRegistry;
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

/**
 * Created by Sara Pellegrini on 18/04/2018. sara.pellegrini@gmail.com
 */
@Component
public class CommandMetricsWebSocket {

    public static final String DESTINATION = "/topic/commands/";
    private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();
    private final CommandMetricsRegistry commandMetricsRegistry;

    private final CommandHandlerRegistry commandHandlerRegistry;

    private final Topology topology;
    private final SimpMessagingTemplate webSocket;

    public CommandMetricsWebSocket(CommandMetricsRegistry commandMetricsRegistry,
                                   CommandHandlerRegistry commandHandlerRegistry,
                                   Topology topology,
                                   SimpMessagingTemplate webSocket) {
        this.commandMetricsRegistry = commandMetricsRegistry;
        this.commandHandlerRegistry = commandHandlerRegistry;
        this.topology = topology;
        this.webSocket = webSocket;
    }

    @Scheduled(initialDelayString = "${axoniq.axonserver.websocket-update.initial-delay:10000}",
            fixedRateString = "${axoniq.axonserver.websocket-update.rate:1000}")
    public void publish() {
        if (subscriptions.isEmpty()) {
            return;
        }
        commandHandlerRegistry.all()
                              .map(this::asCommandMetric)
                              .filter(m -> contexts.contains(m))
                              .doOnNext(commandMetric -> webSocket.convertAndSend(DESTINATION, commandMetric))
                              .subscribe();
    }

    @EventListener
    public void on(SessionSubscribeEvent event) {
        StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
        if (sha.getDestination() != null && sha.getDestination().startsWith(DESTINATION)) {
            subscriptions.put(sha.getDestination(),
                              topology.visibleContexts(false, new PrincipalAuthentication(event.getUser())));
        }
    }

    @EventListener
    public void on(SessionUnsubscribeEvent event) {
        StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
        if (sha.getDestination() != null) {
            subscriptions.remove(sha.getDestination());
        }
    }

    private CommandMetricsRegistry.CommandMetric asCommandMetric(CommandHandler commandHandler) {
        String clientId = commandHandler.metadata()
                                        .metadataValue(CommandHandler.CLIENT_ID, "UNKNOWN");
        String componentName = commandHandler.metadata()
                                             .metadataValue(CommandHandler.COMPONENT_NAME, "UNKNOWN");
        return commandMetricsRegistry.commandMetric(commandHandler.commandName(),
                                                    clientId,
                                                    commandHandler.context(),
                                                    componentName);
    }

    private Stream<CommandMetricsRegistry.CommandMetric> getMetrics(CommandHandler commandHandler,
                                                                    Set<CommandRegistrationCache.RegistrationEntry> registrations) {
        return registrations.stream()
                            .map(registration -> commandMetricsRegistry
                                    .commandMetric(registration.getCommand(),
                                                   commandHandler.getClientId(),
                                                   commandHandler.getClientStreamIdentification().getContext(),
                                                   commandHandler.getComponentName()));
    }

}
