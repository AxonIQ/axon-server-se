/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.grpc.ErrorMessage;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class MessageJson {

    private final String location;
    private final String message;
    private final List<String> details;

    public MessageJson(ErrorMessage message) {
        this.message = message.getMessage();
        this.details = message.getDetailsList();
        this.location = message.getLocation();
    }

    public String getLocation() {
        return location;
    }

    public String getMessage() {
        return message;
    }

    public List<String> getDetails() {
        return details;
    }
}
