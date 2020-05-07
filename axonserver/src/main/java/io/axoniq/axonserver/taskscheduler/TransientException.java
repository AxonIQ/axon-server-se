/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.taskscheduler;

/**
 * Exception to be thrown when a task fails with an error that may disappear by itself (or by restarting a component).
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class TransientException extends RuntimeException {

    /**
     * Creates a {@link TransientException} with just a message.
     *
     * @param message the exception message
     */
    public TransientException(String message) {
        super(message);
    }

    /**
     * Creates a {@link TransientException} with a message and a wrapped cause.
     *
     * @param message the exception message
     * @param cause   the wrapped cause of the exception
     */
    public TransientException(String message, Throwable cause) {
        super(message, cause);
    }
}
