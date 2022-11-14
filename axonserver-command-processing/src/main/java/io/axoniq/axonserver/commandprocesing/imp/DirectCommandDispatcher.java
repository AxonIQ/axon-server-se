/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DirectCommandDispatcher implements CommandDispatcher {

    private final Logger logger = LoggerFactory.getLogger(DirectCommandDispatcher.class);

    @Override
    public Mono<CommandResult> dispatch(CommandHandlerSubscription handler, Command commandRequest) {
        return handler.dispatch(commandRequest)
                      .doFirst(() -> logger.debug("{}: dispatch {} ({}) to {}",
                                                  commandRequest.context(),
                                                  commandRequest.commandName(),
                                                  commandRequest.id(),
                                                  handler.commandHandler().id()))
                      .doOnNext(r -> logger.debug("{}: received result for {} ({})",
                                                  commandRequest.context(),
                                                  commandRequest.commandName(),
                                                  commandRequest.id()));
    }

    @Override
    public Mono<Void> onCommandHandlerUnsubscribed(CommandHandler commandHandler) {
        return Mono.empty();
    }
}
