/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandException;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandFailedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandReceivedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandResultReceivedInterceptor;
import io.axoniq.axonserver.message.command.CommandMetricsRegistry;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CommandDispatcherMetrics {

    public static final String NO_TARGET = "NO-TARGET";

    public static final String NO_SOURCE = "NO-SOURCE";
    private final CommandMetricsRegistry metricRegistry;
    private final Map<String, ActiveCommand> activeCommands = new ConcurrentHashMap<>();

    public CommandDispatcherMetrics(CommandRequestProcessor commandRequestProcessor,
                                    CommandMetricsRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        commandRequestProcessor.registerInterceptor(CommandReceivedInterceptor.class, this::commandReceived);
        commandRequestProcessor.registerInterceptor(CommandResultReceivedInterceptor.class, this::resultReceived);
        commandRequestProcessor.registerInterceptor(CommandFailedInterceptor.class, this::commandFailed);
    }

    private Mono<CommandException> commandFailed(Mono<CommandException> commandExceptionMono) {
        return commandExceptionMono.doOnNext(commandException -> {
            ActiveCommand activeCommand = activeCommands.remove(commandException.command().id());
            long now = System.currentTimeMillis();
            if (activeCommand != null) {
                metricRegistry.add(activeCommand.commandName,
                                   activeCommand.client,
                                   NO_TARGET,
                                   activeCommand.context,
                                   now - activeCommand.start);
            }
        });
    }

    private Mono<CommandResult> resultReceived(Mono<CommandResult> commandResultMono) {
        return commandResultMono.doOnNext(commandResult -> {
            ActiveCommand activeCommand = activeCommands.remove(commandResult.commandId());
            long now = System.currentTimeMillis();
            if (activeCommand == null) {
                return;
            }

            String clientId = commandResult.metadata().metadataValue(CommandResult.CLIENT_ID, NO_TARGET);
            metricRegistry.add(activeCommand.commandName,
                               activeCommand.client,
                               clientId,
                               activeCommand.context,
                               now - activeCommand.start);
        });
    }

    private Mono<Command> commandReceived(Mono<Command> commandMono) {
        return commandMono.doOnNext(command -> {
            Metadata metadata = command.metadata();
            String clientId = metadata.metadataValue(Command.CLIENT_ID, NO_SOURCE);
            activeCommands.put(command.id(), new ActiveCommand(command.commandName(),
                                                               command.context(),
                                                               clientId,
                                                               System.currentTimeMillis()));
        });
    }

    private static class ActiveCommand {

        private final String commandName;
        private final String context;
        private final String client;
        private final long start;

        public ActiveCommand(String commandName, String context, String client, long start) {
            this.commandName = commandName;
            this.context = context;
            this.client = client;
            this.start = start;
        }
    }
}
