/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.messaging;

import io.axoniq.axonserver.exception.ErrorCode;

/**
 * Exceptions created inside AxonServer.
 *
 * @author Marc Gathier
 */
public class MessagingPlatformException extends RuntimeException {

    private final ErrorCode errorCode;

    public MessagingPlatformException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public MessagingPlatformException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public static MessagingPlatformException create(Throwable cause) {
        if (cause instanceof MessagingPlatformException) {
            return (MessagingPlatformException) cause;
        }
        return new MessagingPlatformException(ErrorCode.OTHER, cause.getMessage(), cause);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        return "[" + errorCode.getCode() + "] " + super.getMessage();
    }
}
