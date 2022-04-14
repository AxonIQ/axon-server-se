/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.exception.ErrorCode;
import org.springframework.http.ResponseEntity;

/**
 * @author Marc Gathier
 */
public class RestResponse {
    private final boolean success;
    private final String message;

    public RestResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public ResponseEntity<RestResponse> asResponseEntity(ErrorCode erroCode) {
        return ResponseEntity.status(erroCode.getHttpCode()).body(this);
    }
}
