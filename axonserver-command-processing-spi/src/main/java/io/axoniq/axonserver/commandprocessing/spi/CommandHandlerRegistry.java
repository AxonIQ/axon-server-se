/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Registry containing all the currently registered command handlers.
 *
 * @author Marc Gathier
 * @since 2023.0.0
 */
public interface CommandHandlerRegistry {

    /**
     * Registers a command handler.
     *
     * @param handler the handler to register
     * @return a mono that completes when the registration is competed
     */
    Mono<Void> register(CommandHandlerSubscription handler);

    /**
     * Unregisters a command handler.
     *
     * @param handlerId the identification of the command handler
     * @return a mono that completes when the registration is removed
     */
    Mono<CommandHandler> unregister(String handlerId);

    /**
     * Finds a handler to execute the given {@code command}.
     *
     * @param command the command to execute
     * @return the selected handler for the command
     */
    Mono<CommandHandlerSubscription> handler(Command command);

    /**
     * Retrieves all registered command handlers
     *
     * @return a flux of all registered command handlers
     */
    Flux<CommandHandler> all();

    /**
     * Finds the subscription for a command handler with given {@code handlerId}.
     *
     * @param handlerId the identification of the handler
     * @return the subscription
     */
    CommandHandlerSubscription find(String handlerId);
}
