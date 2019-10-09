/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.exception;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Utilities for exception classes
 *
 * @author Marc Gathier
 * @since 4.2.2
 */
public class ExceptionUtils {

    /**
     * Checks if the given exception is a gRPC CANCELLED exception. This exception occurs when the client cancels the
     * connection
     * (usually on a non-clean exit)
     *
     * @param cause the exception to analyse
     * @return true if the exception is that the connections is cancelled
     */
    public static boolean isCancelled(Throwable cause) {
        return (cause instanceof StatusRuntimeException
                && ((StatusRuntimeException) cause).getStatus().getCode().equals(Status.Code.CANCELLED));
    }
}
