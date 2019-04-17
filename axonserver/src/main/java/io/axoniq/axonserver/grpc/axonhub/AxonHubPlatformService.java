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
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import org.springframework.stereotype.Component;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * Entry point to accept axonhub client connection requests in Axon Server. Difference between Axon Server and AxonHub client is the service name.
 * Delegates the request to the normal (Axon Server) {@link PlatformService}
 * @author Marc Gathier
 */
@Component
public class AxonHubPlatformService implements AxonServerClientService {

    public static final String SERVICE_NAME = "io.axoniq.platform.grpc.PlatformService";
    public static final io.grpc.MethodDescriptor<ClientIdentification,
            PlatformInfo> METHOD_GET_PLATFORM_SERVER =
            io.grpc.MethodDescriptor.newBuilder(io.grpc.protobuf.ProtoUtils
                                                        .marshaller(ClientIdentification.getDefaultInstance()),
                                                io.grpc.protobuf.ProtoUtils
                                                        .marshaller(PlatformInfo.getDefaultInstance()))
                                    .setFullMethodName(generateFullMethodName(
                                            SERVICE_NAME, "GetPlatformServer"))
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY).build();
    public static final io.grpc.MethodDescriptor<PlatformInboundInstruction,
            PlatformOutboundInstruction> METHOD_OPEN_STREAM =
            io.grpc.MethodDescriptor.newBuilder(io.grpc.protobuf.ProtoUtils
                                                        .marshaller(PlatformInboundInstruction.getDefaultInstance()),
                                                io.grpc.protobuf.ProtoUtils
                                                        .marshaller(PlatformOutboundInstruction.getDefaultInstance()))
                                    .setFullMethodName(generateFullMethodName(
                                            SERVICE_NAME, "OpenStream"))
                                    .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
                                    .build();

    private final PlatformService platformService;

    public AxonHubPlatformService(PlatformService platformService) {
        this.platformService = platformService;
    }


    @Override
    public final io.grpc.ServerServiceDefinition bindService() {
        return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_GET_PLATFORM_SERVER,
                                                      asyncUnaryCall(platformService::getPlatformServer))
                                              .addMethod(
                                                      METHOD_OPEN_STREAM,
                                                      asyncBidiStreamingCall(platformService::openStream))
                                              .build();
    }
}
