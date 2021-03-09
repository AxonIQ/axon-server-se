/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
 * @author Marc Gathier
 */
public class NoOpCommandInterceptors implements CommandInterceptors {

    @Override
    public SerializedCommand commandRequest(SerializedCommand serializedCommand,
                                            PluginUnitOfWork unitOfWork) {
        return serializedCommand;
    }

    @Override
    public SerializedCommandResponse commandResponse(SerializedCommandResponse serializedResponse,
                                                     PluginUnitOfWork unitOfWork) {
        return serializedResponse;
    }
}
