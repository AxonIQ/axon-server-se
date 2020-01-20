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
import io.axoniq.axonserver.message.ClientIdentification;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class CommandInformation {
    private final String requestIdentifier;
    private final Consumer<SerializedCommandResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final ClientIdentification clientId;
    private final String componentName;
    private final String sourceClientId;

    public CommandInformation(String requestIdentifier, String sourceClientId,
                              Consumer<SerializedCommandResponse> responseConsumer, ClientIdentification clientId,
                              String componentName) {
        this.requestIdentifier = requestIdentifier;
        this.sourceClientId = sourceClientId;
        this.responseConsumer = responseConsumer;
        this.clientId = clientId;
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

    public ClientIdentification getClientId() {
        return clientId;
    }

    public String getComponentName() {
        return componentName;
    }

    public boolean checkClient(ClientIdentification client) {
        return clientId.equals(client);
    }

    public void cancel() {
        CommandResponse commandResponse = CommandResponse.newBuilder()
                                                         .setMessageIdentifier(UUID.randomUUID().toString())
                                                         .setRequestIdentifier(requestIdentifier)
                                                         .setErrorCode(ErrorCode.COMMAND_TIMEOUT.getCode())
                                                         .setErrorMessage(ErrorMessage.newBuilder().setMessage("Cancelled by AxonServer due to timeout"))
                                                         .build();
        responseConsumer.accept(new SerializedCommandResponse(commandResponse));
    }

    public String getSourceClientId() {
        return sourceClientId;
    }
}
