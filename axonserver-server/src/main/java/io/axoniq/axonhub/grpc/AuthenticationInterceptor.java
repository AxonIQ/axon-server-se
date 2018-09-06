package io.axoniq.axonhub.grpc;

import io.axoniq.axonhub.AxonHubAccessController;
import io.axoniq.axonhub.exception.ErrorCode;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;

/**
 * Author: marc
 */
public class AuthenticationInterceptor implements ServerInterceptor{
    private final AxonHubAccessController axonHubAccessController;

    public AuthenticationInterceptor(AxonHubAccessController axonHubAccessController) {
        this.axonHubAccessController = axonHubAccessController;
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> serverCall, Metadata metadata, ServerCallHandler<T, R> serverCallHandler) {
        String token = metadata.get(GrpcMetadataKeys.TOKEN_KEY);
        StatusRuntimeException sre = null;
        String context = GrpcMetadataKeys.CONTEXT_KEY.get();

        if( token == null) {
            sre = GrpcExceptionBuilder.build(ErrorCode.AUTHENTICATION_TOKEN_MISSING, "Token missing");
        } else if( ! axonHubAccessController.allowed( serverCall.getMethodDescriptor().getFullMethodName(), context, token)) {
            sre = GrpcExceptionBuilder.build(ErrorCode.AUTHENTICATION_INVALID_TOKEN, "Invalid token");
        }

        if( sre != null) {
            serverCall.close(sre.getStatus(), sre.getTrailers());
            return new ServerCall.Listener<T>() {};
        }

        Context updatedGrpcContext = Context.current().withValue(GrpcMetadataKeys.TOKEN_CONTEXT_KEY, token);
        return Contexts.interceptCall(updatedGrpcContext, serverCall, metadata, serverCallHandler);
    }
}
