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
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intercepts clients calls and checks for license validity.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
public class LicenseInterceptor implements ServerInterceptor {

    private static final String APPEND_EVENT = "io.axoniq.axonserver.grpc.event.EventStore/AppendEvent";
    private static final String DISPATCH_COMMAND = "io.axoniq.axonserver.grpc.command.CommandService/Dispatch";

    private final LicenseAccessController licenseAccessController;
    private final Logger logger = LoggerFactory.getLogger(LicenseInterceptor.class);

    public LicenseInterceptor(LicenseAccessController licenseAccessController) {
        this.licenseAccessController = licenseAccessController;
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> serverCall, Metadata metadata, ServerCallHandler<T, R> serverCallHandler) {
        if (APPEND_EVENT.equals(serverCall.getMethodDescriptor().getFullMethodName()) ||
               DISPATCH_COMMAND.equals(serverCall.getMethodDescriptor().getFullMethodName())) {
            if (!licenseAccessController.allowed()) {
                StatusRuntimeException sre = GrpcExceptionBuilder.build(ErrorCode.OTHER, "License is not valid!");
                logger.error("Warning: Unauthorized feature(s) that cannot be used with this license have been detected." +
                        "Please remove them or contact AxonIQ for more information/assistance.");
                serverCall.close(sre.getStatus(), sre.getTrailers());
                return new ServerCall.Listener<T>() {
                };
            }
        }
        return serverCallHandler.startCall(serverCall, metadata);
    }
}
