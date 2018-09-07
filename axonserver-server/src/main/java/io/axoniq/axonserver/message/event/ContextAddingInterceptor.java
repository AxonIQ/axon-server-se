package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.GrpcMetadataKeys;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.util.function.Supplier;

/**
 * Author: marc
 */
public class ContextAddingInterceptor implements ClientInterceptor {

    private final Supplier<String> contextSupplier;

    public ContextAddingInterceptor(Supplier<String> contextSupplier) {
        this.contextSupplier = contextSupplier;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String context = contextSupplier.get();
                if( context != null) headers.put(GrpcMetadataKeys.AXONDB_CONTEXT_MD_KEY, context);
                super.start(responseListener, headers);
            }
        };
    }
}
