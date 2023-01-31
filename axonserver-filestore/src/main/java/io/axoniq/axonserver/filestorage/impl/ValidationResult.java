/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.filestorage.impl;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class ValidationResult {

    private final long segment;
    private final String message;
    private final long lastToken;
    private final boolean valid;

    public ValidationResult(long segment, long lastToken) {
        this.segment = segment;
        this.lastToken = lastToken;
        this.valid = true;
        this.message = null;
    }

    public ValidationResult(long segment, String message) {
        this.segment = segment;
        this.message = message;
        this.valid = false;
        this.lastToken = segment;
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

    public long getSegment() {
        return segment;
    }
}
