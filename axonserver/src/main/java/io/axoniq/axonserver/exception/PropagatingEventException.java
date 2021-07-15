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
 * @author Marc Gathier
 * @since 4.4.13
 */
public class PropagatingEventException extends RuntimeException {

    public PropagatingEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
