/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.InstructionResult;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.message.ClientIdentification;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class DirectCommandHandler extends CommandHandler<SerializedCommandProviderInbound> {
    public DirectCommandHandler(StreamObserver<SerializedCommandProviderInbound> responseObserver, ClientIdentification client, String componentName) {
        super(responseObserver, client, componentName);
    }

    @Override
    public void dispatch(SerializedCommand request) {
        observer.onNext(SerializedCommandProviderInbound.newBuilder().setCommand(request).build());
    }

    @Override
    public void confirm(String messageId) {
        observer.onNext(SerializedCommandProviderInbound.newBuilder()
                                                        .setConfirmation(InstructionResult.newBuilder()
                                                                                          .setSuccess(true)
                                                                                          .setInstructionId(messageId)
                                                                                          .build())
                                                        .build());

    }

}
