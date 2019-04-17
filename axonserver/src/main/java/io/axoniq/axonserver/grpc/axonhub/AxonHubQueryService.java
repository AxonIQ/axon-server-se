/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc.axonhub;

import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.QueryService;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryProviderOutbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.springframework.stereotype.Component;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;

/**
 * Entry point to accept axonhub client queries in Axon Server. Difference between Axon Server and AxonHub client is the service name.
 * Delegates the request to the normal (Axon Server) {@link QueryService}
 *
 * @author Marc Gathier
 */
@Component
public class AxonHubQueryService implements AxonServerClientService {
    public static final String SERVICE_NAME = "io.axoniq.axonhub.grpc.QueryService";

    public static final MethodDescriptor<QueryProviderOutbound, QueryProviderInbound> METHOD_OPEN_STREAM =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(QueryProviderOutbound.getDefaultInstance()),
                                        ProtoUtils.marshaller(QueryProviderInbound.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "OpenStream"))
                            .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                            .build();

    public static final MethodDescriptor<QueryRequest, QueryResponse> METHOD_QUERY =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(QueryRequest.getDefaultInstance()),
                                        ProtoUtils.marshaller(QueryResponse.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Query"))
                            .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                            .build();


    public static final MethodDescriptor<SubscriptionQueryRequest, SubscriptionQueryResponse> METHOD_SUBSCRIPTION =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(SubscriptionQueryRequest.getDefaultInstance()),
                                        ProtoUtils.marshaller(SubscriptionQueryResponse.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Subscription"))
                            .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                            .build();

    private final QueryService queryService;

    public AxonHubQueryService(QueryService queryService) {
        this.queryService = queryService;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(SERVICE_NAME)
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
