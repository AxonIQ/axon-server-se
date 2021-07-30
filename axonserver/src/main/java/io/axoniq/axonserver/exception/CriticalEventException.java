/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.exception;

/**
 * Exception to be thrown when there is a critical error while handling an application event.
 *
 * @author Marc Gathier
 * @since 4.4.13
 */
public class CriticalEventException extends RuntimeException {

    /**
     * @param message describes the exception
     * @param cause a wrapped cause
     */
    public CriticalEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
