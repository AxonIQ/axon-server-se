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
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandException;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandFailedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandReceivedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandResultReceivedInterceptor;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.interceptor.CommandInterceptors;
import io.axoniq.axonserver.interceptor.DefaultExecutionContext;
import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.transport.grpc.command.GrpcCommand;
import io.axoniq.axonserver.transport.grpc.command.GrpcCommandResult;
import io.axoniq.axonserver.transport.grpc.command.GrpcMapper;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.axoniq.axonserver.commandprocessing.spi.Metadata.isInternal;

@Component
public class CommandPluginHandler {

    private final CommandInterceptors commandInterceptors;
    private final Map<String, DefaultExecutionContext> executionContextMap = new ConcurrentHashMap<>();

    public CommandPluginHandler(CommandRequestProcessor commandRequestProcessor,
                                CommandInterceptors commandInterceptors) {
        this.commandInterceptors = commandInterceptors;
        commandRequestProcessor.registerInterceptor(CommandReceivedInterceptor.class, this::receivedCommand);
        commandRequestProcessor.registerInterceptor(CommandResultReceivedInterceptor.class, this::receivedResponse);
        commandRequestProcessor.registerInterceptor(CommandFailedInterceptor.class, this::commandFailed);
    }

    private Mono<CommandException> commandFailed(Mono<CommandException> commandExceptionMono) {
        return commandExceptionMono.doOnNext(commandException -> {
            DefaultExecutionContext context = executionContextMap.remove(commandException.command().id());
            if (context != null) {
                context.compensate(commandException.exception());
            }
        });
    }

    private Mono<CommandResult> receivedResponse(Mono<CommandResult> commandResultMono) {
        return commandResultMono.map(commandResult -> {
            ExecutionContext context = executionContextMap.remove(commandResult.commandId());
            if (context != null) {
                SerializedCommandResponse response = commandInterceptors.commandResponse(new SerializedCommandResponse(
                        GrpcMapper.map(commandResult)), context);
                return new GrpcCommandResult(response.wrapped());
            }
            return commandResult;
        });
    }

    private Mono<Command> receivedCommand(Mono<Command> commandMono) {
        return commandMono.map(command -> {
            DefaultExecutionContext context = new DefaultExecutionContext(command.context(), null);
            executionContextMap.put(command.id(), context);
            Map<String, Serializable> internalMetadata = new HashMap<>();
            command.metadata().metadataKeys().forEach(key -> {
                if (isInternal(key)) {
                    command.metadata().metadataValue(key).ifPresent(value -> internalMetadata.put(key, value));
                }
            });
            SerializedCommand response = commandInterceptors.commandRequest(new SerializedCommand(GrpcMapper.map(
                    command)), context);
            return new GrpcCommand(response.wrapped(), command.context(), internalMetadata, null);
        });
    }
}
