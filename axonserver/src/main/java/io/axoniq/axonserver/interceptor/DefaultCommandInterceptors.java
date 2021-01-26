/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.RequestRejectedException;
import io.axoniq.axonserver.extensions.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final Logger logger = LoggerFactory.getLogger(DefaultCommandInterceptors.class);

    private final ExtensionContextFilter extensionContextFilter;


    public DefaultCommandInterceptors(ExtensionContextFilter extensionContextFilter) {
        this.extensionContextFilter = extensionContextFilter;
    }

    @Override
    public SerializedCommand commandRequest(SerializedCommand serializedCommand,
                                            ExtensionUnitOfWork extensionUnitOfWork) {
        List<CommandRequestInterceptor> commandRequestInterceptors = extensionContextFilter.getServicesForContext(
                CommandRequestInterceptor.class,
                extensionUnitOfWork.context());
        if (commandRequestInterceptors.isEmpty()) {
            return serializedCommand;
        }
        Command command = serializedCommand.wrapped();
        for (CommandRequestInterceptor commandRequestInterceptor : commandRequestInterceptors) {
            try {
                command = commandRequestInterceptor.commandRequest(command, extensionUnitOfWork);
            } catch (RequestRejectedException e) {
                e.printStackTrace();
            }
        }
        return new SerializedCommand(command);
    }

    @Override
    public SerializedCommandResponse commandResponse(SerializedCommandResponse serializedResponse,
                                                     ExtensionUnitOfWork extensionUnitOfWork) {
        List<CommandResponseInterceptor> commandResponseInterceptors = extensionContextFilter.getServicesForContext(
                CommandResponseInterceptor.class,
                extensionUnitOfWork.context());
        if (commandResponseInterceptors.isEmpty()) {
            return serializedResponse;
        }
        CommandResponse response = serializedResponse.wrapped();
        try {
            for (CommandResponseInterceptor commandResponseInterceptor : commandResponseInterceptors) {
                response = commandResponseInterceptor.commandResponse(response, extensionUnitOfWork);
            }
        } catch (Exception ex) {
            logger.warn("{}: an exception occurred in a CommandResponseInterceptor",
                        extensionUnitOfWork.context(), ex);
        }

        return new SerializedCommandResponse(response);
    }
}
