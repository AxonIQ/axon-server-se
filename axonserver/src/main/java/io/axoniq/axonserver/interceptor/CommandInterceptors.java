/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.plugin.PluginUnitOfWork;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;

/**
 * Bundles the interceptors for commands.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public interface CommandInterceptors {

    /**
     * Invokes all {@link io.axoniq.axonserver.plugin.interceptor.CommandRequestInterceptor} instances. Interceptors
     * may change the content of the command.
     *
     * @param serializedCommand the command to intercept
     * @param unitOfWork        the unit of work for the command
     * @return the command after executing the interceptors
     */
    SerializedCommand commandRequest(SerializedCommand serializedCommand,
                                     PluginUnitOfWork unitOfWork);

    /**
     * Invokes all {@link io.axoniq.axonserver.plugin.interceptor.CommandResponseInterceptor} instances.
     * Interceptors may change the content of the response.
     *
     * @param serializedResponse the response to intercept
     * @param unitOfWork         the unit of work for the command
     * @return the response after executing the interceptors
     */
    SerializedCommandResponse commandResponse(SerializedCommandResponse serializedResponse,
                                              PluginUnitOfWork unitOfWork);
}
