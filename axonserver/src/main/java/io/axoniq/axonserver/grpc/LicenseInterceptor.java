/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.LicenseAccessController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intercepts clients calls and checks for license validity.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
public class LicenseInterceptor implements ServerInterceptor {
    private final LicenseAccessController licenseAccessController;
    private final Logger logger = LoggerFactory.getLogger(LicenseInterceptor.class);

    public LicenseInterceptor(LicenseAccessController licenseAccessController) {
        this.licenseAccessController = licenseAccessController;
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> serverCall, Metadata metadata, ServerCallHandler<T, R> serverCallHandler) {

        if (!licenseAccessController.allowed()) {
            StatusRuntimeException sre = GrpcExceptionBuilder.build(ErrorCode.OTHER, "License is not valid!");
            logger.error("License is not valid!");
            serverCall.close(sre.getStatus(), sre.getTrailers());
            return new ServerCall.Listener<T>() {
            };
        }

        return serverCallHandler.startCall(serverCall, metadata);
    }
}
