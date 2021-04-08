/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.refactoring.configuration.topology.Topology;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Interceptor that retrieves the CONTEXT from the request metadata and adds it in the threadlocal gRPC context.
 *
 * @author Marc Gathier
 */
public class ContextInterceptor implements ServerInterceptor{
    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> serverCall, Metadata metadata, ServerCallHandler<T, R> serverCallHandler) {
        String context = metadata.get(GrpcMetadataKeys.CONTEXT_MD_KEY);
        if( context == null) context = metadata.get(GrpcMetadataKeys.AXONDB_CONTEXT_MD_KEY);
        if( context == null) context = Topology.DEFAULT_CONTEXT;
        Context updatedGrpcContext = Context.current().withValue(GrpcMetadataKeys.CONTEXT_KEY, context);
        return Contexts.interceptCall(updatedGrpcContext, serverCall, metadata, serverCallHandler);
    }
}
