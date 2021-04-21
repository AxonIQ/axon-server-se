/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.refactoring.messaging.command.CommandInterceptors;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;

/**
 * @author Marc Gathier
 */
public class NoOpCommandInterceptors implements CommandInterceptors {

    @Override
    public Command commandRequest(Command serializedCommand,
                                  ExecutionContext executionContext) {
        return serializedCommand;
    }

    @Override
    public CommandResponse commandResponse(CommandResponse serializedResponse,
                                           ExecutionContext unitOfWork) {
        return serializedResponse;
    }
}
