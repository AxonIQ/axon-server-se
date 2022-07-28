/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.command;

import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerSubscribedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Component
public class CommandSubscriptionCache {

    private final Map<String, Set<CommandHandler>> commandHandlerMap = new ConcurrentHashMap<>();

    public CommandSubscriptionCache(CommandRequestProcessor commandRequestProcessor) {
        commandRequestProcessor.registerInterceptor(CommandHandlerUnsubscribedInterceptor.class, this::unsubscribed);
        commandRequestProcessor.registerInterceptor(CommandHandlerSubscribedInterceptor.class, this::subscribed);
    }

    private Mono<Void> unsubscribed(CommandHandler commandHandler) {
        return commandHandler.metadata().metadataValue(CommandHandler.COMPONENT_NAME)
                             .doOnNext(componentName -> commandHandlerMap.get(String.valueOf(componentName))
                                                                         .remove(commandHandler))
                             .then();
    }

    private Mono<Void> subscribed(CommandHandler commandHandler) {
        return commandHandler.metadata().metadataValue(CommandHandler.COMPONENT_NAME)
                             .doOnNext(componentName -> commandHandlerMap.computeIfAbsent(String.valueOf(componentName),
                                                                                          c -> new CopyOnWriteArraySet<>())
                                                                         .add(commandHandler))
                             .then();
    }

    public Set<CommandHandler> get(String component) {
        return commandHandlerMap.getOrDefault(component, Collections.emptySet());
    }
}
