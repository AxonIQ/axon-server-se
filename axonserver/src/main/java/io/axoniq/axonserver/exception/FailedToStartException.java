/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.exception;

import org.springframework.boot.ExitCodeGenerator;

public class FailedToStartException extends MessagingPlatformException implements ExitCodeGenerator {

    public FailedToStartException(String message, Throwable cause) {
        super(ErrorCode.OTHER, message, cause);
    }

    public FailedToStartException(String message) {
        super(ErrorCode.OTHER, message);
    }

    @Override
    public int getExitCode() {
        return -1;
    }
}
