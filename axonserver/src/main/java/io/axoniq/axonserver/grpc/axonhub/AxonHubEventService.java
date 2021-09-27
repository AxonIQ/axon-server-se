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
import io.axoniq.axonserver.grpc.EventStoreService;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.*;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.event.SerializedEventMarshaller;
import io.axoniq.axonserver.message.event.SerializedEventWithTokenMarshaller;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.springframework.stereotype.Component;

import java.io.InputStream;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.*;

/**
 *  Entry point to accept axonhub client eventstore requests in Axon Server. Difference between Axon Server and AxonHub client is the service name.
 *  Delegates the request to the normal (Axon Server) {@link EventDispatcher}r.
 * @author Marc Gathier
 */
@Component
public class AxonHubEventService implements AxonServerClientService {
    public static final String SERVICE_NAME = "io.axoniq.axondb.grpc.EventStore";

    public static final MethodDescriptor<SerializedEvent, Confirmation> METHOD_APPEND_EVENT =
            MethodDescriptor.newBuilder(new SerializedEventMarshaller(),
                                        ProtoUtils.marshaller(Confirmation.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AppendEvent"))
                            .setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
                            .build();

    
    public static final MethodDescriptor<Event, Confirmation> METHOD_APPEND_SNAPSHOT =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(Event.getDefaultInstance()),
                                        ProtoUtils.marshaller(Confirmation.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AppendSnapshot"))
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .build();

    public static final MethodDescriptor<GetAggregateEventsRequest, SerializedEvent> METHOD_LIST_AGGREGATE_EVENTS =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(GetAggregateEventsRequest.getDefaultInstance()),
                                        SerializedEventMarshaller.serializedEventMarshaller())
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListAggregateEvents"))
                            .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                            .build();
    
    public static final MethodDescriptor<GetEventsRequest, SerializedEventWithToken> METHOD_LIST_EVENTS =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(GetEventsRequest.getDefaultInstance()),
                                        new SerializedEventWithTokenMarshaller())
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListEvents"))
                            .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                            .build();

    public static final MethodDescriptor<ReadHighestSequenceNrRequest, ReadHighestSequenceNrResponse> METHOD_READ_HIGHEST_SEQUENCE_NR =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(ReadHighestSequenceNrRequest.getDefaultInstance()),
                                        ProtoUtils.marshaller(ReadHighestSequenceNrResponse.getDefaultInstance()))
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadHighestSequenceNr"))
                            .build();


    public static final MethodDescriptor<QueryEventsRequest, QueryEventsResponse> METHOD_QUERY_EVENTS =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(QueryEventsRequest.getDefaultInstance()),
                                        ProtoUtils.marshaller(QueryEventsResponse.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryEvents"))
                            .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                            .build();

    public static final MethodDescriptor<GetFirstTokenRequest, TrackingToken> METHOD_GET_FIRST_TOKEN =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(GetFirstTokenRequest.getDefaultInstance()),
                                        ProtoUtils.marshaller(TrackingToken.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFirstToken"))
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .build();

    public static final MethodDescriptor<GetLastTokenRequest,TrackingToken> METHOD_GET_LAST_TOKEN =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(GetLastTokenRequest.getDefaultInstance()),
                                        ProtoUtils.marshaller(TrackingToken.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetLastToken"))
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .build();

    public static final MethodDescriptor<GetTokenAtRequest, TrackingToken> METHOD_GET_TOKEN_AT =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(GetTokenAtRequest.getDefaultInstance()),
                                        ProtoUtils.marshaller(TrackingToken.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTokenAt"))
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .build();


    private final EventStoreService eventDispatcher;

    public AxonHubEventService(EventStoreService eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_APPEND_EVENT,
                                                      asyncClientStreamingCall( eventDispatcher::appendEvent))
                                              .addMethod(
                                                      METHOD_APPEND_SNAPSHOT,
                                                      asyncUnaryCall(
                                                              eventDispatcher::appendSnapshot))
                                              .addMethod(
                                                      METHOD_LIST_AGGREGATE_EVENTS,
                                                      asyncServerStreamingCall(eventDispatcher::listAggregateEvents))
                                              .addMethod(
                                                      METHOD_LIST_EVENTS,
                                                      asyncBidiStreamingCall(eventDispatcher::listEvents))
                                              .addMethod(
                                                      METHOD_READ_HIGHEST_SEQUENCE_NR,
                                                      asyncUnaryCall(eventDispatcher::readHighestSequenceNr))
                                              .addMethod(
                                                      METHOD_GET_FIRST_TOKEN,
                                                      asyncUnaryCall(eventDispatcher::getFirstToken))
                                              .addMethod(
                                                      METHOD_GET_LAST_TOKEN,
                                                      asyncUnaryCall(eventDispatcher::getLastToken))
                                              .addMethod(
                                                      METHOD_GET_TOKEN_AT,
                                                      asyncUnaryCall(eventDispatcher::getTokenAt))
                                              .addMethod(
                                                      METHOD_QUERY_EVENTS,
                                                      asyncBidiStreamingCall(eventDispatcher::queryEvents))
                                              .build();

    }
}
