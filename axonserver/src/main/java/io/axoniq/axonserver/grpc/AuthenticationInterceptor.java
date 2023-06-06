/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
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
import io.axoniq.axonserver.logging.AuditLog;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;
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
        Authentication authentication = null;
        if (token == null) {
            AuditLog.getLogger().warn("{}: Request without token sent from {}",
                                      context,
                                      serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
            sre = GrpcExceptionBuilder.build(ErrorCode.AUTHENTICATION_TOKEN_MISSING,
                                             "No token for " + serverCall.getMethodDescriptor().getFullMethodName());
        } else {
            authentication = authentication(token);
            if (!axonServerAccessController.allowed(serverCall.getMethodDescriptor().getFullMethodName(),
                                                    context,
                                                    authentication)) {
                AuditLog.getLogger().warn("{}: Request with invalid token for {} sent from {}",
                                          context,
                                          serverCall.getMethodDescriptor().getFullMethodName(),
                                          serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
                sre = GrpcExceptionBuilder.build(ErrorCode.AUTHENTICATION_INVALID_TOKEN,
                                                 "Invalid token for " + serverCall.getMethodDescriptor()
                                                                                  .getFullMethodName());
            }
        }

        if (sre != null) {
            serverCall.close(sre.getStatus(), sre.getTrailers());
            return new ServerCall.Listener<T>() {
            };
        }
        Context updatedGrpcContext = Context.current()
                                            .withValue(GrpcMetadataKeys.PRINCIPAL_CONTEXT_KEY,
                                                       authentication);
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

    private Authentication authentication(String token) {
        Authentication authentication;
        try {
            authentication = axonServerAccessController.authenticate(token);
        } catch (InvalidTokenException invalidTokenException) {
            authentication = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
        }
        return authentication;
    }
}
