/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

/**
 * @author Marc Gathier
 */
public class CommandExecutionException extends RuntimeException {

    private final int errorCode;
    private final String url;

    public CommandExecutionException(int errorCode, String url, String errorMessage) {
        super(errorMessage);
        this.errorCode = errorCode;
        this.url = url;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getUrl() {
        return url;
    }
}
