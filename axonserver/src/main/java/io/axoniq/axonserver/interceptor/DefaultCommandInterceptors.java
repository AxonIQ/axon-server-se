/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.RequestRejectedException;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.extensions.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.metric.MeterFactory;
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

    private final ExtensionContextFilter extensionContextFilter;
    private final InterceptorTimer interceptorTimer;


    public DefaultCommandInterceptors(ExtensionContextFilter extensionContextFilter,
                                      MeterFactory meterFactory) {
        this.extensionContextFilter = extensionContextFilter;
        this.interceptorTimer = new InterceptorTimer(meterFactory);
    }

    @Override
    public SerializedCommand commandRequest(SerializedCommand serializedCommand,
                                            ExtensionUnitOfWork extensionUnitOfWork) {
        List<ServiceWithInfo<CommandRequestInterceptor>> commandRequestInterceptors = extensionContextFilter
                .getServicesWithInfoForContext(
                        CommandRequestInterceptor.class,
                        extensionUnitOfWork.context());
        if (commandRequestInterceptors.isEmpty()) {
            return serializedCommand;
        }
        Command intercepted =
                interceptorTimer.time(extensionUnitOfWork.context(), "CommandRequestInterceptor", () -> {
                    Command command = serializedCommand.wrapped();
                    for (ServiceWithInfo<CommandRequestInterceptor> commandRequestInterceptor :
                            commandRequestInterceptors) {
                        try {
                            command = commandRequestInterceptor.service().commandRequest(command,
                                                                                         extensionUnitOfWork);
                        } catch (RequestRejectedException requestRejectedException) {
                            throw new MessagingPlatformException(ErrorCode.COMMAND_REJECTED_BY_INTERCEPTOR,
                                                                 extensionUnitOfWork.context() +
                                                                         ": Command rejected by the CommandRequestInterceptor in "
                                                                         + commandRequestInterceptor.extensionKey(),
                                                                 requestRejectedException);
                        } catch (Exception interceptorException) {
                            throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                 extensionUnitOfWork.context() +
                                                                         ": Exception thrown by the CommandRequestInterceptor in "
                                                                         + commandRequestInterceptor.extensionKey(),
                                                                 interceptorException);
                        }
                    }
                    return command;
                });
        return new SerializedCommand(intercepted);
    }

    @Override
    public SerializedCommandResponse commandResponse(SerializedCommandResponse serializedResponse,
                                                     ExtensionUnitOfWork extensionUnitOfWork) {
        List<ServiceWithInfo<CommandResponseInterceptor>> commandResponseInterceptors = extensionContextFilter
                .getServicesWithInfoForContext(
                        CommandResponseInterceptor.class,
                        extensionUnitOfWork.context());
        if (commandResponseInterceptors.isEmpty()) {
            return serializedResponse;
        }
        CommandResponse intercepted = interceptorTimer.time(extensionUnitOfWork.context(),
                                                            "CommandResponseInterceptor", () -> {
                    CommandResponse response = serializedResponse.wrapped();
                    for (ServiceWithInfo<CommandResponseInterceptor> commandResponseInterceptor : commandResponseInterceptors) {
                        try {
                            response = commandResponseInterceptor.service().commandResponse(response,
                                                                                            extensionUnitOfWork);
                        } catch (Exception interceptorException) {
                            throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                 extensionUnitOfWork.context() +
                                                                         ": Exception thrown by the CommandRequestInterceptor in "
                                                                         + commandResponseInterceptor.extensionKey(),
                                                                 interceptorException);
                        }
                    }
                    return response;
                });

        return new SerializedCommandResponse(intercepted);
    }
}
