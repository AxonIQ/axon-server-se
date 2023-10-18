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
import io.axoniq.axonserver.exception.MessagingPlatformException;

/**
 * @author Stefan Dragisic
 */
public class InsufficientBufferCapacityException extends MessagingPlatformException {

    public InsufficientBufferCapacityException(String errorMessage) {
        super(ErrorCode.TOO_MANY_REQUESTS, errorMessage);
    }

}
