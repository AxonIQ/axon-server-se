/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.exception;

/**
 * @author Marc Gathier
 */
public class EventStoreValidationException extends MessagingPlatformException {

    public EventStoreValidationException(String message) {
        super(ErrorCode.INVALID_TRANSACTION_TOKEN, message);
    }
}
