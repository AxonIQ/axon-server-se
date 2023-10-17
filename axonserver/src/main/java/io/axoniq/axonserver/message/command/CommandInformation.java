/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
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
    private final CommandHandler commandHandler;
    private final Consumer<SerializedCommandResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final String sourceClientId;

    public CommandInformation(String requestIdentifier, String sourceClientId,
                              CommandHandler commandHandler,
                              Consumer<SerializedCommandResponse> responseConsumer) {
        this.requestIdentifier = requestIdentifier;
        this.sourceClientId = sourceClientId;
        this.commandHandler = commandHandler;
        this.responseConsumer = responseConsumer;
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
        return commandHandler.getClientStreamIdentification();
    }

    public String getComponentName() {
        return commandHandler.getComponentName();
    }

    public boolean checkClient(ClientStreamIdentification client) {
        return commandHandler.getClientStreamIdentification().equals(client);
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
        commandHandler.cancel(requestIdentifier);
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
        return commandHandler.getClientId();
    }
}
