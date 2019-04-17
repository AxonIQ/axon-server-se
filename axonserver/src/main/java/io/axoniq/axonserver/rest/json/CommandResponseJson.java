/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.grpc.command.CommandResponse;

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
