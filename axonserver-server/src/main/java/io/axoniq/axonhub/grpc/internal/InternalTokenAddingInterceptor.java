package io.axoniq.axonhub.grpc.internal;

import io.axoniq.axonhub.grpc.GrpcMetadataKeys;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Author: marc
 */
public class InternalTokenAddingInterceptor implements ClientInterceptor {
    private final String token;

    public InternalTokenAddingInterceptor(String token) {
        this.token = token;
    }

    @Override
    public <T, R> ClientCall<T, R> interceptCall(MethodDescriptor<T, R> methodDescriptor, CallOptions callOptions, Channel channel) {

        return new ForwardingClientCall.SimpleForwardingClientCall<T, R>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(Listener<R> responseListener, Metadata headers) {
                if( token != null) headers.put(GrpcMetadataKeys.INTERNAL_TOKEN_KEY, token);
                super.start(responseListener, headers);
            }
        };
    }
}
