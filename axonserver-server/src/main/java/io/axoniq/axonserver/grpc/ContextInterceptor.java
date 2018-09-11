package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.enterprise.context.ContextController;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Author: marc
 */
public class ContextInterceptor implements ServerInterceptor{
    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> serverCall, Metadata metadata, ServerCallHandler<T, R> serverCallHandler) {
        String context = metadata.get(GrpcMetadataKeys.CONTEXT_MD_KEY);
        if( context == null) context = ContextController.DEFAULT;
        Context updatedGrpcContext = Context.current().withValue(GrpcMetadataKeys.CONTEXT_KEY, context);
        return Contexts.interceptCall(updatedGrpcContext, serverCall, metadata, serverCallHandler);
    }
}
