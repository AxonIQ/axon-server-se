/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.plugin.ExecutionContext;

/**
 * @author Marc Gathier
 */
public class NoOpCommandInterceptors implements CommandInterceptors {

    @Override
    public SerializedCommand commandRequest(SerializedCommand serializedCommand,
                                            ExecutionContext executionContext) {
        return serializedCommand;
    }

    @Override
    public SerializedCommandResponse commandResponse(SerializedCommandResponse serializedResponse,
                                                     ExecutionContext unitOfWork) {
        return serializedResponse;
    }

    @Override
    public boolean noRequestInterceptors(String context) {
        return true;
    }

    @Override
    public boolean noResponseInterceptors(String context) {
        return true;
    }
}
