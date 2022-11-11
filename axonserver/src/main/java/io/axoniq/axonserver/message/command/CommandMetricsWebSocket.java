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
import io.axoniq.axonserver.message.SubscriptionKey;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by Sara Pellegrini on 18/04/2018. sara.pellegrini@gmail.com
 */
@Component
public class CommandMetricsWebSocket {

    public static final String DESTINATION = "/topic/commands";
    private final Set<SubscriptionKey> subscriptions = new CopyOnWriteArraySet<>();
    private final CommandMetricsRegistry commandMetricsRegistry;

    private final CommandHandlerRegistry commandHandlerRegistry;

    private final SimpMessagingTemplate webSocket;

    public CommandMetricsWebSocket(CommandMetricsRegistry commandMetricsRegistry,
                                   CommandHandlerRegistry commandHandlerRegistry,
                                   SimpMessagingTemplate webSocket) {
        this.commandMetricsRegistry = commandMetricsRegistry;
        this.commandHandlerRegistry = commandHandlerRegistry;
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
                                .doOnNext(commandMetric -> webSocket.convertAndSend(DESTINATION, commandMetric))
                                .subscribe();
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
}
