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
import io.axoniq.axonserver.grpc.CommandService;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.message.ByteArrayMarshaller;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.springframework.stereotype.Component;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * Entry point to accept axonhub client commands in Axon Server. Difference between Axon Server and AxonHub client is the service name.
 * Delegates the request to the normal (Axon Server) {@link CommandService}
 * @author Marc Gathier
 */
@Component
public class AxonHubCommandService implements AxonServerClientService {
    private static final String SERVICE_NAME = "io.axoniq.axonhub.grpc.CommandService";
    private static final MethodDescriptor<CommandProviderOutbound, SerializedCommandProviderInbound> METHOD_OPEN_STREAM =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(CommandProviderOutbound.getDefaultInstance()),
                                        ProtoUtils.marshaller(SerializedCommandProviderInbound.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(
                                    SERVICE_NAME, "OpenStream"))
                            .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                            .build();

    private static final MethodDescriptor<byte[], SerializedCommandResponse> METHOD_DISPATCH =
            MethodDescriptor.newBuilder(ByteArrayMarshaller.instance(),
                                        ProtoUtils.marshaller(SerializedCommandResponse.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Dispatch"))
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .build();

    private final CommandService commandService;

    public AxonHubCommandService(CommandService commandService) {
        this.commandService = commandService;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_OPEN_STREAM,
                                                      asyncBidiStreamingCall(commandService::openStream))
                                              .addMethod(
                                                      METHOD_DISPATCH,
                                                      asyncUnaryCall(commandService::dispatch))
                                              .build();
    }
}
