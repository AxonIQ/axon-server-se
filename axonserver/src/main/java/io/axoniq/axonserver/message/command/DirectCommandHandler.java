/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class DirectCommandHandler extends CommandHandler<SerializedCommandProviderInbound> {

    private final FlowControlQueues<WrappedCommand> flowControlQueues;

    public DirectCommandHandler(StreamObserver<SerializedCommandProviderInbound> responseObserver,
                                ClientStreamIdentification clientStreamIdentification,
                                FlowControlQueues<WrappedCommand> flowControlQueues,
                                String clientId,
                                String componentName) {
        super(responseObserver, clientStreamIdentification, clientId, componentName);
        this.flowControlQueues = flowControlQueues;
    }

    @Override
    public void dispatch(SerializedCommand request) {
        observer.onNext(SerializedCommandProviderInbound.newBuilder().setCommand(request).build());
    }

    @Override
    public void confirm(String messageId) {
        observer.onNext(SerializedCommandProviderInbound.newBuilder()
                                                        .setAcknowledgement(InstructionAck.newBuilder()
                                                                                          .setSuccess(true)
                                                                                          .setInstructionId(messageId)
                                                                                          .build())
                                                        .build());
    }

    @Override
    public void send(WrappedCommand wrappedCommand) {
        flowControlQueues.put(clientStreamIdentification, wrappedCommand, wrappedCommand.priority());
    }
}
