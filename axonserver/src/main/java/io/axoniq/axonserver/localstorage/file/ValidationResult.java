/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

/**
 * @author Marc Gathier
 */
public class ValidationResult {
    private final long segment;
    private final long lastToken;
    private final boolean valid;
    private final String message;

    public ValidationResult(long segment, long lastToken) {
        this.segment = segment;
        this.lastToken = lastToken;
        this.valid = true;
        this.message = null;
    }
    public ValidationResult(long segment, String message) {
        this.segment = segment;
        this.lastToken = -1;
        this.valid = false;
        this.message = message;
    }

    public long getSegment() {
        return segment;
    }

    public long getLastToken() {
        return lastToken;
    }

    public boolean isValid() {
        return valid;
    }

    public String getMessage() {
        return message;
    }
}
