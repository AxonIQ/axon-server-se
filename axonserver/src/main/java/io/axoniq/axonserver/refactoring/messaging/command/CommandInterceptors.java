/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.messaging.command;

import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;

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
     * @param command          the command to intercept
     * @param executionContext the execution context for the command
     * @return the command after executing the interceptors
     */
    Command commandRequest(Command command,
                           ExecutionContext executionContext);

    /**
     * Invokes all {@link io.axoniq.axonserver.plugin.interceptor.CommandResponseInterceptor} instances.
     * Interceptors may change the content of the response.
     *
     * @param response   the response to intercept
     * @param unitOfWork the execution context for the command
     * @return the response after executing the interceptors
     */
    CommandResponse commandResponse(CommandResponse response,
                                    ExecutionContext unitOfWork);
}
