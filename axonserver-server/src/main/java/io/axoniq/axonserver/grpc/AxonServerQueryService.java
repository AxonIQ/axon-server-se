package io.axoniq.axonserver.grpc;

import io.axoniq.axonhub.QueryRequest;
import io.axoniq.axonhub.QueryResponse;
import io.axoniq.axonhub.SubscriptionQueryRequest;
import io.axoniq.axonhub.SubscriptionQueryResponse;
import io.axoniq.axonhub.grpc.QueryProviderInbound;
import io.axoniq.axonhub.grpc.QueryProviderOutbound;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.springframework.stereotype.Component;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;

/**
 * Author: marc
 */
@Component
public class AxonServerQueryService implements AxonServerClientService {
    public static final String SERVICE_NAME = "io.axoniq.axonserver.grpc.query.QueryService";

    public static final io.grpc.MethodDescriptor<io.axoniq.axonhub.grpc.QueryProviderOutbound,
            io.axoniq.axonhub.grpc.QueryProviderInbound> METHOD_OPEN_STREAM =
            io.grpc.MethodDescriptor.newBuilder(ProtoUtils.marshaller(QueryProviderOutbound.getDefaultInstance()), ProtoUtils.marshaller(QueryProviderInbound
                                                        .getDefaultInstance()))
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "OpenStream"))
                                    .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                                                .build();

    public static final io.grpc.MethodDescriptor<io.axoniq.axonhub.QueryRequest,
            io.axoniq.axonhub.QueryResponse> METHOD_QUERY =
            io.grpc.MethodDescriptor.newBuilder(ProtoUtils.marshaller(QueryRequest.getDefaultInstance()), ProtoUtils.marshaller(QueryResponse
                                                                                                                                                 .getDefaultInstance()))
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Query"))
                                    .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                                    .build();


    public static final io.grpc.MethodDescriptor<io.axoniq.axonhub.SubscriptionQueryRequest,
            io.axoniq.axonhub.SubscriptionQueryResponse> METHOD_SUBSCRIPTION =
            io.grpc.MethodDescriptor.newBuilder(ProtoUtils.marshaller(SubscriptionQueryRequest.getDefaultInstance()), ProtoUtils.marshaller(SubscriptionQueryResponse
                                                                                                                                        .getDefaultInstance()))
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Subscription"))
                                    .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                                    .build();

    private final QueryService queryService;

    public AxonServerQueryService(QueryService queryService) {
        this.queryService = queryService;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_OPEN_STREAM,
                                                      asyncBidiStreamingCall( queryService::openStream))
                                              .addMethod(
                                                      METHOD_QUERY,
                                                      asyncServerStreamingCall( queryService::query))
                                              .addMethod(
                                                      METHOD_SUBSCRIPTION,
                                                      asyncBidiStreamingCall( queryService::subscription))
                                              .build();
    }
}
