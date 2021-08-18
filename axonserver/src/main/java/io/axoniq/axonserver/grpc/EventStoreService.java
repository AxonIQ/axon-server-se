/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventStoreGrpc;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.GetFirstTokenRequest;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.GetTokenAtRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.event.ForwardingStreamObserver;
import io.axoniq.axonserver.message.event.InputStreamMarshaller;
import io.axoniq.axonserver.message.event.SequenceValidationStrategy;
import io.axoniq.axonserver.message.event.SequenceValidationStreamObserver;
import io.axoniq.axonserver.message.event.SerializedEventMarshaller;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.io.InputStream;

import static io.grpc.stub.ServerCalls.*;

/**
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @author Stefan Dragisic
 */
@Component
public class EventStoreService implements AxonServerClientService {

    public static final MethodDescriptor<GetEventsRequest, InputStream> METHOD_LIST_EVENTS =
            EventStoreGrpc.getListEventsMethod().toBuilder(
                                  ProtoUtils.marshaller(GetEventsRequest.getDefaultInstance()),
                                  InputStreamMarshaller.inputStreamMarshaller())
                    .build();
    public static final MethodDescriptor<GetAggregateEventsRequest, SerializedEvent> METHOD_LIST_AGGREGATE_EVENTS =
            EventStoreGrpc.getListAggregateEventsMethod().toBuilder(
                                  ProtoUtils.marshaller(GetAggregateEventsRequest.getDefaultInstance()),
                                  SerializedEventMarshaller.serializedEventMarshaller())
                    .build();
    public static final MethodDescriptor<GetAggregateSnapshotsRequest, SerializedEvent> METHOD_LIST_AGGREGATE_SNAPSHOTS =
            EventStoreGrpc.getListAggregateSnapshotsMethod().toBuilder(
                    ProtoUtils.marshaller(GetAggregateSnapshotsRequest.getDefaultInstance()),
                    SerializedEventMarshaller.serializedEventMarshaller())
                    .build();
    public static final MethodDescriptor<InputStream, Confirmation> METHOD_APPEND_EVENT =
            EventStoreGrpc.getAppendEventMethod().toBuilder(
                    InputStreamMarshaller.inputStreamMarshaller(),
                    ProtoUtils.marshaller(Confirmation.getDefaultInstance()))
                    .build();
    private final Logger logger = LoggerFactory.getLogger(EventStoreService.class);
    @Value("${axoniq.axonserver.read-sequence-validation-strategy:LOG}")
    private SequenceValidationStrategy sequenceValidationStrategy = SequenceValidationStrategy.LOG;
    private final AuthenticationProvider authenticationProvider;
    private final ContextProvider contextProvider;
    private final EventDispatcher eventDispatcher;

    public EventStoreService(ContextProvider contextProvider,
                             AuthenticationProvider authenticationProvider,
                             EventDispatcher eventDispatcher) {
        this.contextProvider = contextProvider;
        this.authenticationProvider = authenticationProvider;
        this.eventDispatcher = eventDispatcher;
    }


    public StreamObserver<InputStream> appendEvent(StreamObserver<Confirmation> responseObserver) {
        CallStreamObserver<Confirmation> callStreamObserver = (CallStreamObserver<Confirmation>) responseObserver;
        return eventDispatcher.appendEvent(contextProvider.getContext(), authenticationProvider.get(),
                new ForwardingStreamObserver<>(logger, "appendEvent", callStreamObserver));
    }


    public void appendSnapshot(Event snapshot, StreamObserver<Confirmation> streamObserver) {
        ForwardingStreamObserver<Confirmation> responseObserver =
                new ForwardingStreamObserver<>(logger,
                                               "appendSnapshot",
                                               (CallStreamObserver<Confirmation>) streamObserver);
        eventDispatcher.appendSnapshot(contextProvider.getContext(), snapshot, authenticationProvider.get())
                       .doOnSuccess(v -> {
                           responseObserver.onNext(Confirmation.newBuilder()
                                                               .setSuccess(true)
                                                               .build());
                           responseObserver.onCompleted();
                       })
                       .doOnError(responseObserver::onError)
                       .doOnCancel(() -> responseObserver.onError(MessagingPlatformException
                                                                          .create(new RuntimeException(
                                                                                  "Appending snapshot cancelled"))))
                       .subscribe();

    }

    public void listAggregateEvents(GetAggregateEventsRequest request,
                                    StreamObserver<SerializedEvent> responseObserver) {
        CallStreamObserver<SerializedEvent> streamObserver = (CallStreamObserver<SerializedEvent>) responseObserver;
        String context = contextProvider.getContext();
        CallStreamObserver<SerializedEvent> validateStreamObserver =
                new SequenceValidationStreamObserver(streamObserver, sequenceValidationStrategy, context);
        eventDispatcher.listAggregateEvents(context, authenticationProvider.get(),
                            request,
                            new ForwardingStreamObserver<>(logger, "listAggregateEvents", validateStreamObserver));
    }


    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<InputStream> responseObserver) {
        return eventDispatcher.listEvents(contextProvider.getContext(), authenticationProvider.get(), responseObserver);
    }

    @Override
    public final io.grpc.ServerServiceDefinition bindService() {
        return io.grpc.ServerServiceDefinition.builder(EventStoreGrpc.SERVICE_NAME)
                .addMethod(
                        METHOD_APPEND_EVENT,
                        asyncClientStreamingCall(this::appendEvent))
                .addMethod(
                        EventStoreGrpc.getAppendSnapshotMethod(),
                        asyncUnaryCall(this::appendSnapshot))
                .addMethod(
                        METHOD_LIST_AGGREGATE_EVENTS,
                        asyncServerStreamingCall(this::listAggregateEvents))
                .addMethod(
                        METHOD_LIST_AGGREGATE_SNAPSHOTS,
                        asyncServerStreamingCall(this::listAggregateSnapshots))
                .addMethod(
                        METHOD_LIST_EVENTS,
                        asyncBidiStreamingCall(this::listEvents))
                .addMethod(
                        EventStoreGrpc.getReadHighestSequenceNrMethod(),
                        asyncUnaryCall(this::readHighestSequenceNr))
                .addMethod(
                        EventStoreGrpc.getGetFirstTokenMethod(),
                        asyncUnaryCall(this::getFirstToken))
                .addMethod(
                        EventStoreGrpc.getGetLastTokenMethod(),
                        asyncUnaryCall(this::getLastToken))
                .addMethod(
                        EventStoreGrpc.getGetTokenAtMethod(),
                        asyncUnaryCall(this::getTokenAt))
                .addMethod(
                        EventStoreGrpc.getQueryEventsMethod(),
                        asyncBidiStreamingCall(this::queryEvents))
                .build();
    }

    public void getFirstToken(GetFirstTokenRequest request, StreamObserver<TrackingToken> streamObserver) {
        CallStreamObserver<TrackingToken> callStreamObserver = (CallStreamObserver<TrackingToken>) streamObserver;
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
                "getFirstToken",
                callStreamObserver);
        eventDispatcher.getFirstToken(contextProvider.getContext(), responseObserver);
    }

    public void getLastToken(GetLastTokenRequest request, StreamObserver<TrackingToken> streamObserver) {
        CallStreamObserver<TrackingToken> callStreamObserver = (CallStreamObserver<TrackingToken>) streamObserver;
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
                "getLastToken",
                callStreamObserver);
        eventDispatcher.getLastToken(contextProvider.getContext(), responseObserver);
    }

    public void getTokenAt(GetTokenAtRequest request, StreamObserver<TrackingToken> streamObserver) {
        CallStreamObserver<TrackingToken> callStreamObserver = (CallStreamObserver<TrackingToken>) streamObserver;
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
                "getTokenAt",
                callStreamObserver);
        eventDispatcher.getTokenAt(contextProvider.getContext(), request.getInstant(), responseObserver);
    }

    public void readHighestSequenceNr(ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> streamObserver) {
        CallStreamObserver<ReadHighestSequenceNrResponse> callStreamObserver = (CallStreamObserver<ReadHighestSequenceNrResponse>) streamObserver;
        ForwardingStreamObserver<ReadHighestSequenceNrResponse> responseObserver =
                new ForwardingStreamObserver<>(logger, "readHighestSequenceNr", callStreamObserver);
        eventDispatcher.readHighestSequenceNr(contextProvider.getContext(), request.getAggregateId(), responseObserver);
    }

    public StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> streamObserver) {
        CallStreamObserver<QueryEventsResponse> callStreamObserver = (CallStreamObserver<QueryEventsResponse>) streamObserver;
        String context = contextProvider.getContext();
        Authentication authentication = authenticationProvider.get();
        ForwardingStreamObserver<QueryEventsResponse> responseObserver =
                new ForwardingStreamObserver<>(logger, "queryEvents", callStreamObserver);
        return eventDispatcher.queryEvents(context, authentication, streamObserver);
    }

    public void listAggregateSnapshots(GetAggregateSnapshotsRequest request,
                                       StreamObserver<SerializedEvent> responseObserver) {
        eventDispatcher.listAggregateSnapshots(contextProvider.getContext(), authenticationProvider.get(), request, responseObserver);
    }

}
