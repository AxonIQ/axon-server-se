/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;

/**
 * @author Marc Gathier
 */
public class DirectCommandHandler extends CommandHandler {

    private final FlowControlQueues<WrappedCommand> commandQueues;

    public DirectCommandHandler(ClientIdentification client,
                                String componentName,
                                FlowControlQueues<WrappedCommand> commandQueues) {
        super(client, componentName);
        this.commandQueues = commandQueues;
    }

    @Override
    public void dispatch(SerializedCommand request) {
        commandQueues.put(client.toString(), new WrappedCommand(client, request));
    }
}
