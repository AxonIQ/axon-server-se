/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.command.CommandResponse;

import java.util.UUID;

import static io.axoniq.axonserver.util.StringUtils.getOrDefault;

/**
 * @author Marc Gathier
 */
public class CommandResponseJson {

    private final String requestIdentifier;
    private final String errorCode;
    private final MessageJson errorMessage;
    private final SerializedObjectJson payload;
    private final String messageIdentifier;
    private MetaDataJson metaData;

    public CommandResponseJson(CommandResponse r) {
        requestIdentifier = r.getRequestIdentifier();
        errorCode = r.getErrorCode();
        errorMessage = r.hasErrorMessage() ? new MessageJson(r.getErrorMessage()) : null;
        payload = r.hasPayload() ? new SerializedObjectJson(r.getPayload()) : null;
        messageIdentifier = r.getMessageIdentifier();
        metaData = new MetaDataJson(r.getMetaDataMap());
    }

    public CommandResponseJson(CommandResult r) {
        requestIdentifier = r.commandId();
        errorMessage = null;
        if (r.payload() != null) {
            errorCode = r.payload().error() ? "ERROR" : null;
//            errorMessage = r.payload().error() ? new MessageJson(r.getErrorMessage()) : null;
            payload = r.payload().type() != null ? new SerializedObjectJson(r.payload()) : null;
        } else {
            errorCode = null;
            payload = null;
        }
        messageIdentifier = r.id();
        metaData = new MetaDataJson(r.metadata());
    }

    public CommandResponseJson(String r, Throwable e) {
        requestIdentifier = r;
        errorMessage = new MessageJson(ErrorMessage.newBuilder()
                                                   .setMessage(getOrDefault(e.getMessage(), "Null"))
                                                   .build());
        errorCode = MessagingPlatformException.create(e).getErrorCode().getCode();
        payload = null;
        messageIdentifier = UUID.randomUUID().toString();
        metaData = new MetaDataJson();
    }

    public String getRequestIdentifier() {
        return requestIdentifier;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public MessageJson getErrorMessage() {
        return errorMessage;
    }

    public SerializedObjectJson getPayload() {
        return payload;
    }

    public String getMessageIdentifier() {
        return messageIdentifier;
    }

    public MetaDataJson getMetaData() {
        return metaData;
    }
}
