package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.GrpcMetadataKeys;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;
import org.springframework.stereotype.Controller;


/**
 * Created by marc on 7/17/2017.
 */
@Controller("InternalAuthenticationInterceptor")
public class InternalAuthenticationInterceptor implements ServerInterceptor {
    private final MessagingPlatformConfiguration configuration;
    public InternalAuthenticationInterceptor(MessagingPlatformConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> serverCall, Metadata metadata,
                                                       ServerCallHandler<T, R> serverCallHandler) {
        String token = metadata.get(GrpcMetadataKeys.INTERNAL_TOKEN_KEY);
        StatusRuntimeException sre = null;
        if( token == null) {
            sre = GrpcExceptionBuilder.build(ErrorCode.AUTHENTICATION_TOKEN_MISSING, "Missing internal token");
        } else if( ! token.equals(configuration.getAccesscontrol().getInternalToken())) {
            sre = GrpcExceptionBuilder.build(ErrorCode.AUTHENTICATION_INVALID_TOKEN, "Invalid token");
        }

        if( sre != null) {
            serverCall.close(sre.getStatus(), sre.getTrailers());
            return new ServerCall.Listener<T>() {};
        }

        return serverCallHandler.startCall(serverCall, metadata);
    }
}
