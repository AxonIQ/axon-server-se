/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.config.OsgiController;
import io.axoniq.axonserver.extensions.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.extensions.interceptor.InterceptorContext;
import io.axoniq.axonserver.extensions.interceptor.OrderedInterceptor;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultCommandInterceptors implements CommandInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultCommandInterceptors.class);

    private final List<CommandRequestInterceptor> commandRequestInterceptors = new CopyOnWriteArrayList<>();
    private final List<CommandResponseInterceptor> commandResponseInterceptors = new CopyOnWriteArrayList<>();
    private final OsgiController osgiController;
    private volatile boolean initialized;


    public DefaultCommandInterceptors(OsgiController osgiController) {
        this.osgiController = osgiController;
    }

    private void initialize() {
        if (!initialized) {
            synchronized (osgiController) {
                if (initialized) {
                    return;
                }

                osgiController.getServices(CommandRequestInterceptor.class).forEach(commandRequestInterceptors::add);
                osgiController.getServices(CommandResponseInterceptor.class).forEach(commandResponseInterceptors::add);
                commandRequestInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                commandResponseInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                initialized = true;

                logger.warn("{} commandRequestInterceptors", commandRequestInterceptors.size());
                logger.warn("{} commandResponseInterceptors", commandResponseInterceptors.size());
            }
        }
    }

    @Override
    public SerializedCommand commandRequest(InterceptorContext interceptorContext,
                                            SerializedCommand serializedCommand) {
        initialize();
        if (commandRequestInterceptors.isEmpty()) {
            return serializedCommand;
        }
        Command command = serializedCommand.wrapped();
        for (CommandRequestInterceptor commandRequestInterceptor : commandRequestInterceptors) {
            command = commandRequestInterceptor.commandRequest(interceptorContext, command);
        }
        return new SerializedCommand(command);
    }

    @Override
    public SerializedCommandResponse commandResponse(InterceptorContext interceptorContext,
                                                     SerializedCommandResponse serializedResponse) {
        initialize();
        if (commandResponseInterceptors.isEmpty()) {
            return serializedResponse;
        }
        CommandResponse response = serializedResponse.wrapped();
        for (CommandResponseInterceptor commandResponseInterceptor : commandResponseInterceptors) {
            response = commandResponseInterceptor.commandResponse(interceptorContext, response);
        }
        return new SerializedCommandResponse(response);
    }
}
