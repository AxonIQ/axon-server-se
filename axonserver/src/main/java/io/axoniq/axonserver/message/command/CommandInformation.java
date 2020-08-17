/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.message.ClientStreamIdentification;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class CommandInformation {
    private final String requestIdentifier;
    private final Consumer<SerializedCommandResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final ClientStreamIdentification clientStreamIdentification;
    private final String componentName;
    private final String sourceClientId;
    private final String targetClientId;

    public CommandInformation(String requestIdentifier, String sourceClientId,
                              String targetClientId, Consumer<SerializedCommandResponse> responseConsumer,
                              ClientStreamIdentification clientStreamIdentification,
                              String componentName) {
        this.requestIdentifier = requestIdentifier;
        this.sourceClientId = sourceClientId;
        this.targetClientId = targetClientId;
        this.responseConsumer = responseConsumer;
        this.clientStreamIdentification = clientStreamIdentification;
        this.componentName = componentName;
    }

    public String getRequestIdentifier() {
        return requestIdentifier;
    }

    public Consumer<SerializedCommandResponse> getResponseConsumer() {
        return responseConsumer;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ClientStreamIdentification getClientStreamIdentification() {
        return clientStreamIdentification;
    }

    public String getComponentName() {
        return componentName;
    }

    public boolean checkClient(ClientStreamIdentification client) {
        return clientStreamIdentification.equals(client);
    }

    public void cancel() {
        CommandResponse commandResponse = CommandResponse.newBuilder()
                                                         .setMessageIdentifier(UUID.randomUUID().toString())
                                                         .setRequestIdentifier(requestIdentifier)
                                                         .setErrorCode(ErrorCode.COMMAND_TIMEOUT.getCode())
                                                         .setErrorMessage(ErrorMessage.newBuilder().setMessage(
                                                                 "Cancelled by AxonServer due to timeout"))
                                                         .build();
        responseConsumer.accept(new SerializedCommandResponse(commandResponse));
    }

    /**
     * Returns the unique client identifier that sent the command request.
     *
     * @return the unique client identifier that sent the command request.
     */
    public String getSourceClientId() {
        return sourceClientId;
    }


    /**
     * Returns the unique identifier of the target client for the command request.
     *
     * @return the unique identifier of the target client for the command request.
     */
    public String getTargetClientId() {
        return targetClientId;
    }
}
