/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.InvalidTokenException;
import io.grpc.*;
import org.springframework.security.core.Authentication;

/**
 * Interceptor around gRPC request to perform authentication/authorization on gRPC requests.
 * The interceptor retrieves the TOKEN and the CONTEXT from gRPC metadata provided in the request.
 * @author Marc Gathier
 */
public class AuthenticationInterceptor implements ServerInterceptor {
    private final AxonServerAccessController axonServerAccessController;

    public AuthenticationInterceptor(AxonServerAccessController axonServerAccessController) {
        this.axonServerAccessController = axonServerAccessController;
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> serverCall, Metadata metadata, ServerCallHandler<T, R> serverCallHandler) {
        String token = token(metadata);
        String context = context(metadata);
        StatusRuntimeException sre = null;

        if (token == null) {
            sre = GrpcExceptionBuilder.build(ErrorCode.AUTHENTICATION_TOKEN_MISSING,
                                             "No token for " + serverCall.getMethodDescriptor().getFullMethodName());
        } else if (!axonServerAccessController.allowed(serverCall.getMethodDescriptor().getFullMethodName(),
                                                       context,
                                                       token)) {
            sre = GrpcExceptionBuilder.build(ErrorCode.AUTHENTICATION_INVALID_TOKEN,
                                             "Invalid token for " + serverCall.getMethodDescriptor()
                                                                              .getFullMethodName());
        }

        if (sre != null) {
            serverCall.close(sre.getStatus(), sre.getTrailers());
            return new ServerCall.Listener<T>() {
            };
        }
        Context updatedGrpcContext = Context.current()
                                            .withValue(GrpcMetadataKeys.PRINCIPAL_CONTEXT_KEY,
                                                       authentication(context, token));
        return Contexts.interceptCall(updatedGrpcContext, serverCall, metadata, serverCallHandler);
    }

    private String context(Metadata metadata) {
        String context = GrpcMetadataKeys.CONTEXT_KEY.get();
        if (context == null) {
            context = metadata.get(GrpcMetadataKeys.AXONDB_CONTEXT_MD_KEY);
        }
        return context;
    }

    private String token(Metadata metadata) {
        String token = metadata.get(GrpcMetadataKeys.TOKEN_KEY);
        if (token == null) {
            token = metadata.get(GrpcMetadataKeys.AXONDB_TOKEN_KEY);
        }
        return token;
    }

    private Authentication authentication(String context, String token) {
        Authentication authentication;
        try {
            authentication = axonServerAccessController.authentication(context, token);
        } catch (InvalidTokenException invalidTokenException) {
            authentication = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
        }
        return authentication;
    }
}
