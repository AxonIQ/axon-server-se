/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandException;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandFailedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandReceivedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandResultReceivedInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ActiveCommandsChecker {

    private final Logger logger = LoggerFactory.getLogger(ActiveCommandsChecker.class);

    private final AtomicInteger activeCommands = new AtomicInteger();


    public ActiveCommandsChecker(CommandRequestProcessor commandRequestProcessor) {
        commandRequestProcessor.registerInterceptor(CommandReceivedInterceptor.class, this::receivedCommand);
        commandRequestProcessor.registerInterceptor(CommandResultReceivedInterceptor.class, this::receivedResponse);
        commandRequestProcessor.registerInterceptor(CommandFailedInterceptor.class, this::commandFailed);
    }

    private Mono<CommandException> commandFailed(Mono<CommandException> commandExceptionMono) {
        return commandExceptionMono.doOnNext(r -> {
            if (!(r.exception() instanceof InsufficientBufferCapacityException)) {
                activeCommands.decrementAndGet();
            }
        });
    }

    private Mono<CommandResult> receivedResponse(Mono<CommandResult> commandResultMono) {
        return commandResultMono.doOnNext(r -> {
            logger.debug("{}: Received response for command {}", r.id(), r.commandId());
            activeCommands.decrementAndGet();
        });
    }

    private Mono<Command> receivedCommand(Mono<Command> commandMono) {
        return commandMono.flatMap(command -> {
            logger.debug("{}: Received command {} - {}", command.context(), command.commandName(), command.id());
            if (activeCommands.get() > 10) {
                return Mono.error(new InsufficientBufferCapacityException("Too many active commands"));
            }
            activeCommands.incrementAndGet();
            return Mono.just(command);
        });
    }
}
