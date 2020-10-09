/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.util.ObjectUtils;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultCommandInterceptors implements CommandInterceptors {

    private final List<CommandRequestInterceptor> commandRequestInterceptors;
    private final List<CommandResponseInterceptor> commandResponseInterceptors;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public DefaultCommandInterceptors(
            @Nullable List<CommandRequestInterceptor> commandRequestInterceptors,
            @Nullable List<CommandResponseInterceptor> commandResponseInterceptors) {
        this.commandRequestInterceptors = ObjectUtils.getOrDefault(commandRequestInterceptors, Collections.emptyList());
        this.commandResponseInterceptors = ObjectUtils.getOrDefault(commandResponseInterceptors,
                                                                    Collections.emptyList());
    }

    @Override
    public SerializedCommand commandRequest(InterceptorContext interceptorContext,
                                            SerializedCommand serializedCommand) {
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
