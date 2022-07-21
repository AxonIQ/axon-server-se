/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.rest.json.RestResponse;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class ResponseEntityFactory {

    public static ResponseEntity<RestResponse> asAcceptedRequest(String s) {
        return ResponseEntity.accepted()
                             .body(new RestResponse(true,
                                                    s));
    }

    public static ResponseEntity<RestResponse> asSuccessResponse(String s) {
        return ResponseEntity.ok()
                             .body(new RestResponse(true,
                                                    s));
    }

    public static Mono<ResponseEntity<RestResponse>> asFailedResponse(Throwable ex) {
        return Mono.just(new RestResponse(false, ex.getMessage())
                                 .asResponseEntity(ErrorCode.fromException(ex)));
    }
}
