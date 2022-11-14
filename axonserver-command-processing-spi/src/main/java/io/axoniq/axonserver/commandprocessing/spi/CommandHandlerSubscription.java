/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Mono;

import java.io.Serializable;

/**
 * Describes a subscription for a command handler. The subscription contains the handler information and a method to
 * dispatch a command to the handler.
 *
 * @author Milan Savic
 * @since 2023.0.0
 */
public interface CommandHandlerSubscription extends Serializable {

    /**
     * Returns the command handler description for this subscription.
     *
     * @return the command handler description for this subscription
     */
    CommandHandler commandHandler();

    /**
     * Dispatches a command to the handler for this subscription.
     *
     * @param command the command to dispatch
     * @return a mono where the result will be published
     */
    Mono<CommandResult> dispatch(Command command);
}
