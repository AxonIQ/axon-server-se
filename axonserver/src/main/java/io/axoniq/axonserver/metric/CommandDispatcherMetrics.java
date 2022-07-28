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

    public static final String NO_TARGET_VALUE = "NO-TARGET";

    public static final Mono<String> NO_TARGET = Mono.just(NO_TARGET_VALUE);
    public static final Mono<String> NO_SOURCE = Mono.just("NO-SOURCE");
    public static final Mono<String> NO_COMPONENT_NAME = Mono.just("NO-COMPONENT-NAME");
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
                                   NO_TARGET_VALUE,
                                   activeCommand.context,
                                   now - activeCommand.start);
            }
        });
    }

    private Mono<CommandResult> resultReceived(Mono<CommandResult> commandResultMono) {
        return commandResultMono.flatMap(commandResult -> {
            ActiveCommand activeCommand = activeCommands.remove(commandResult.commandId());
            long now = System.currentTimeMillis();
            if (activeCommand == null) {
                return Mono.just(commandResult);
            }

            return commandResult.metadata()
                                .metadataValue(CommandResult.CLIENT_ID)
                                .switchIfEmpty(NO_TARGET)
                                .map(target -> {
                                    metricRegistry.add(activeCommand.commandName,
                                                       activeCommand.client,
                                                       (String) target,
                                                       activeCommand.context,
                                                       now - activeCommand.start);
                                    return commandResult;
                                });
        });
    }

    private Mono<Command> commandReceived(Mono<Command> commandMono) {
        return commandMono.flatMap(command -> {
            Metadata metadata = command.metadata();
            return Mono.zip(metadata.metadataValue(Command.CLIENT_ID).switchIfEmpty(NO_SOURCE),
                            metadata.metadataValue(Command.COMPONENT_NAME).switchIfEmpty(NO_COMPONENT_NAME),
                            (clientId, componentName) -> {
                                activeCommands.put(command.id(), new ActiveCommand(command.commandName(),
                                                                                   command.context(),
                                                                                   (String) clientId,
                                                                                   (String) componentName,
                                                                                   System.currentTimeMillis()));
                                return command;
                            });
        });
    }

    private static class ActiveCommand {

        private final String commandName;
        private final String context;
        private final String client;
        private final String component;
        private final long start;

        public ActiveCommand(String commandName, String context, String client, String component, long start) {
            this.commandName = commandName;
            this.context = context;
            this.client = client;
            this.component = component;
            this.start = start;
        }
    }
}
