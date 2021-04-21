/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.messaging.command;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.plugin.RequestRejectedException;
import io.axoniq.axonserver.plugin.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.refactoring.messaging.MessagingPlatformException;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import io.axoniq.axonserver.refactoring.metric.MeterFactory;
import io.axoniq.axonserver.refactoring.plugin.InterceptorTimer;
import io.axoniq.axonserver.refactoring.plugin.PluginContextFilter;
import io.axoniq.axonserver.refactoring.plugin.ServiceWithInfo;
import io.axoniq.axonserver.refactoring.transport.Mapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Bundles the interceptors for commands in a single component.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class DefaultCommandInterceptors implements CommandInterceptors {

    private final PluginContextFilter pluginContextFilter;
    private final Mapper<Command, SerializedCommand> commandMapper;
    private final Mapper<CommandResponse, SerializedCommandResponse> commandResponseMapper;
    private final InterceptorTimer interceptorTimer;


    public DefaultCommandInterceptors(PluginContextFilter pluginContextFilter,
                                      @Qualifier("commandMapper") Mapper<Command, SerializedCommand> commandMapper,
                                      @Qualifier("commandResponseMapper") Mapper<CommandResponse, SerializedCommandResponse> commandResponseMapper,
                                      MeterFactory meterFactory) {
        this.pluginContextFilter = pluginContextFilter;
        this.commandMapper = commandMapper;
        this.commandResponseMapper = commandResponseMapper;
        this.interceptorTimer = new InterceptorTimer(meterFactory);
    }

    @Override
    public Command commandRequest(Command originalCommand,
                                  ExecutionContext executionContext) {
        List<ServiceWithInfo<CommandRequestInterceptor>> commandRequestInterceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        CommandRequestInterceptor.class,
                        executionContext.contextName());
        if (commandRequestInterceptors.isEmpty()) {
            return originalCommand;
        }
        SerializedCommand serializedCommand = commandMapper.map(originalCommand);
        io.axoniq.axonserver.grpc.command.Command intercepted =
                interceptorTimer.time(executionContext.contextName(), "CommandRequestInterceptor", () -> {
                    io.axoniq.axonserver.grpc.command.Command command = serializedCommand.wrapped();
                    for (ServiceWithInfo<CommandRequestInterceptor> commandRequestInterceptor :
                            commandRequestInterceptors) {
                        try {
                            command = commandRequestInterceptor.service().commandRequest(command,
                                                                                         executionContext);
                        } catch (RequestRejectedException requestRejectedException) {
                            throw new MessagingPlatformException(ErrorCode.COMMAND_REJECTED_BY_INTERCEPTOR,
                                                                 executionContext.contextName() +
                                                                         ": Command rejected by the CommandRequestInterceptor in "
                                                                         + commandRequestInterceptor.pluginKey(),
                                                                 requestRejectedException);
                        } catch (Exception interceptorException) {
                            throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                 executionContext.contextName() +
                                                                         ": Exception thrown by the CommandRequestInterceptor in "
                                                                         + commandRequestInterceptor.pluginKey(),
                                                                 interceptorException);
                        }
                    }
                    return command;
                });
        return commandMapper.unmap(new SerializedCommand(intercepted, originalCommand.context()));
    }

    @Override
    public CommandResponse commandResponse(CommandResponse originalResponse,
                                           ExecutionContext unitOfWork) {
        List<ServiceWithInfo<CommandResponseInterceptor>> commandResponseInterceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        CommandResponseInterceptor.class,
                        unitOfWork.contextName());
        if (commandResponseInterceptors.isEmpty()) {
            return originalResponse;
        }

        SerializedCommandResponse serializedCommandResponse = commandResponseMapper.map(originalResponse);
        io.axoniq.axonserver.grpc.command.CommandResponse intercepted = interceptorTimer.time(unitOfWork.contextName(),
                                                                                              "CommandResponseInterceptor",
                                                                                              () -> {
                                                                                                  io.axoniq.axonserver.grpc.command.CommandResponse response = serializedCommandResponse
                                                                                                          .wrapped();
                                                                                                  for (ServiceWithInfo<CommandResponseInterceptor> commandResponseInterceptor : commandResponseInterceptors) {
                                                                                                      try {
                                                                                                          response = commandResponseInterceptor
                                                                                                                  .service()
                                                                                                                  .commandResponse(
                                                                                                                          response,
                                                                                                                          unitOfWork);
                                                                                                      } catch (Exception interceptorException) {
                                                                                                          throw new MessagingPlatformException(
                                                                                                                  ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                                                  unitOfWork
                                                                                                                          .contextName()
                                                                                                                          +
                                                                                                                          ": Exception thrown by the CommandRequestInterceptor in "
                                                                         + commandResponseInterceptor.pluginKey(),
                                                                 interceptorException);
                        }
                    }
                    return response;
                });

        return commandResponseMapper.unmap(new SerializedCommandResponse(intercepted));
    }
}
