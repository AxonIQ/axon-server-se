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
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class DirectCommandHandler extends CommandHandler<SerializedCommandProviderInbound> {

    private final FlowControlQueues<WrappedCommand> commandQueues;

    public DirectCommandHandler(StreamObserver<SerializedCommandProviderInbound> responseObserver,
                                ClientIdentification client, String componentName,
                                FlowControlQueues<WrappedCommand> commandQueues) {
        super(responseObserver, client, componentName);
        this.commandQueues = commandQueues;
    }

    @Override
    public void dispatch(SerializedCommand request) {
        commandQueues.put(queueName(), new WrappedCommand(client, request));
    }
}
