package io.axoniq.axonserver.enterprise.cluster.internal;

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
 * @author Marc Gathier
 */
public class ContextAddingInterceptor implements ClientInterceptor {

    private final Supplier<String> contextSupplier;

    public ContextAddingInterceptor(Supplier<String> contextSupplier) {
        this.contextSupplier = contextSupplier;
    }

    @Override
    public <T, R> ClientCall<T, R> interceptCall(MethodDescriptor<T, R> methodDescriptor, CallOptions callOptions, Channel channel) {

        return new ForwardingClientCall.SimpleForwardingClientCall<T, R>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(Listener<R> responseListener, Metadata headers) {
                String context = contextSupplier.get();
                if( context != null) headers.put(GrpcMetadataKeys.CONTEXT_MD_KEY, context);
                super.start(responseListener, headers);
            }
        };
    }
}
