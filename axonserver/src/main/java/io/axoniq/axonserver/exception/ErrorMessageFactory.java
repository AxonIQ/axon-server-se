/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.exception;

import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.util.StringUtils;

/**
 * Creates error messages.
 *
 * @author Marc Gathier
 */
public class ErrorMessageFactory {
    public static ErrorMessage build(String message) {
        return build(message, "AxonServer");
    }

    public static ErrorMessage build(String message, String location) {
        return ErrorMessage.newBuilder()
                           .setLocation(location)
                           .setMessage(StringUtils.getOrDefault(message, "null"))
                           .build();
    }
}
