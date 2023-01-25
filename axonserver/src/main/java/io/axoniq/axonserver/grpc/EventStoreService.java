/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ExceptionUtils;
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
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.event.ForwardingStreamObserver;
import io.axoniq.axonserver.message.event.SequenceValidationStrategy;
import io.axoniq.axonserver.message.event.SequenceValidationStreamObserver;
import io.axoniq.axonserver.message.event.SerializedEventMarshaller;
import io.axoniq.axonserver.message.event.SerializedEventWithTokenMarshaller;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.axoniq.flowcontrol.OutgoingStream;
import io.axoniq.flowcontrol.producer.grpc.FlowControlledOutgoingStream;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class EventStoreService implements AxonServerClientService {

    public static final MethodDescriptor<GetEventsRequest, SerializedEventWithToken> METHOD_LIST_EVENTS =
            EventStoreGrpc.getListEventsMethod().toBuilder(
                                  ProtoUtils.marshaller(GetEventsRequest.getDefaultInstance()),
                                  new SerializedEventWithTokenMarshaller())
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
    public static final MethodDescriptor<SerializedEvent, Confirmation> METHOD_APPEND_EVENT =
            EventStoreGrpc.getAppendEventMethod().toBuilder(
                                  SerializedEventMarshaller.serializedEventMarshaller(),
                    ProtoUtils.marshaller(Confirmation.getDefaultInstance()))
                    .build();
    private final Logger logger = LoggerFactory.getLogger(EventStoreService.class);
    @Value("${axoniq.axonserver.read-sequence-validation-strategy:LOG}")
    private SequenceValidationStrategy sequenceValidationStrategy = SequenceValidationStrategy.LOG;
    private final AuthenticationProvider authenticationProvider;
    private final ContextProvider contextProvider;
    private final EventDispatcher eventDispatcher;
    private final GrpcFlowControlExecutorProvider grpcFlowControlExecutorProvider;
    private final AxonServerAccessController axonServerAccessController;

    public EventStoreService(ContextProvider contextProvider,
                             AuthenticationProvider authenticationProvider,
                             EventDispatcher eventDispatcher,
                             GrpcFlowControlExecutorProvider grpcFlowControlExecutorProvider,
                             AxonServerAccessController axonServerAccessController) {
        this.contextProvider = contextProvider;
        this.authenticationProvider = authenticationProvider;
        this.eventDispatcher = eventDispatcher;
        this.grpcFlowControlExecutorProvider = grpcFlowControlExecutorProvider;
        this.axonServerAccessController = axonServerAccessController;
    }


    public StreamObserver<SerializedEvent> appendEvent(StreamObserver<Confirmation> responseObserver) {
        Sinks.Many<SerializedEvent> sink = Sinks.many()
                                                .unicast()
                                                .onBackpressureBuffer();

        StreamObserver<SerializedEvent> inputStreamObserver = new StreamObserver<SerializedEvent>() {
            @Override
            public void onNext(SerializedEvent inputStream) {
                try {
                    sink.tryEmitNext(inputStream).orThrow();
                } catch (Exception exception) {
                    sink.tryEmitError(exception);
                    StreamObserverUtils.error(responseObserver, MessagingPlatformException.create(exception));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("Error on connection from client: {}", throwable.getMessage());
                sink.tryEmitError(throwable);
            }

            @Override
            public void onCompleted() {
                sink.tryEmitComplete();
            }
        };

        eventDispatcher.appendEvent(contextProvider.getContext(), authenticationProvider.get(),
                                    sink.asFlux()).subscribe(new BaseSubscriber<Void>() {
            @Override
            protected void hookOnComplete() {
                responseObserver.onNext(Confirmation.newBuilder()
                                                    .setSuccess(true)
                                                    .build());
                responseObserver.onCompleted();
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                responseObserver.onError(GrpcExceptionBuilder.build(throwable));
            }
        });

        return inputStreamObserver;
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
        String context = contextProvider.getContext();
        CallStreamObserver<SerializedEvent> validateStreamObserver =
                new SequenceValidationStreamObserver((CallStreamObserver<SerializedEvent>) responseObserver,
                                                     sequenceValidationStrategy,
                                                     context);
        Executor executor = grpcFlowControlExecutorProvider.provide();
        OutgoingStream<SerializedEvent> outgoingStream = new FlowControlledOutgoingStream<>(validateStreamObserver,
                                                                                            executor);
        outgoingStream.accept(eventDispatcher.aggregateEvents(context, authenticationProvider.get(), request));
    }


    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<SerializedEventWithToken> responseObserver) {
        Executor executor = grpcFlowControlExecutorProvider.provide();
        OutgoingStream<SerializedEventWithToken> outgoingStream = new FlowControlledOutgoingStream<>((CallStreamObserver<SerializedEventWithToken>) responseObserver,
                                                                                                     executor);
        return new StreamObserver<GetEventsRequest>() {

            private final AtomicReference<Sinks.Many<GetEventsRequest>> requestFluxRef = new AtomicReference<>();

            @Override
            public void onNext(GetEventsRequest getEventsRequest) {
                if (requestFluxRef.compareAndSet(null, Sinks.many().unicast().onBackpressureBuffer())) {
                    String context = contextProvider.getContext();
                    Sinks.Many<GetEventsRequest> requestFlux = requestFluxRef.get();
                    if (requestFlux != null) {
                        outgoingStream.accept(eventDispatcher.events(context,
                                                                     authenticationProvider.get(),
                                                                     requestFlux.asFlux()));
                    }
                }
                Sinks.Many<GetEventsRequest> requestFlux = requestFluxRef.get();
                if (requestFlux != null && requestFlux.tryEmitNext(getEventsRequest).isFailure()) {
                    onError(new RuntimeException("Unable to emit request for events."));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                Sinks.Many<GetEventsRequest> requestFlux = requestFluxRef.get();
                if (requestFlux != null) {
                    requestFlux.tryEmitError(throwable);
                }
            }

            @Override
            public void onCompleted() {
                Sinks.Many<GetEventsRequest> requestFlux = requestFluxRef.get();
                if (requestFlux != null) {
                    requestFlux.tryEmitComplete();
                }
            }
        };
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

        eventDispatcher.firstEventToken(contextProvider.getContext())
                       .map(token -> TrackingToken.newBuilder().setToken(token).build())
                       .subscribe(responseObserver::onNext,
                                  responseObserver::onError,
                                  responseObserver::onCompleted);
    }

    public void getLastToken(GetLastTokenRequest request, StreamObserver<TrackingToken> streamObserver) {
        CallStreamObserver<TrackingToken> callStreamObserver = (CallStreamObserver<TrackingToken>) streamObserver;
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
                "getLastToken",
                callStreamObserver);

        eventDispatcher.lastEventToken(contextProvider.getContext())
                       .map(token -> TrackingToken.newBuilder().setToken(token).build())
                       .subscribe(responseObserver::onNext,
                                  responseObserver::onError,
                                  responseObserver::onCompleted);
    }

    public void getTokenAt(GetTokenAtRequest request, StreamObserver<TrackingToken> streamObserver) {
        CallStreamObserver<TrackingToken> callStreamObserver = (CallStreamObserver<TrackingToken>) streamObserver;
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
                "getTokenAt",
                callStreamObserver);

        eventDispatcher.eventTokenAt(contextProvider.getContext(), Instant.ofEpochMilli(request.getInstant()))
                       .map(token -> TrackingToken.newBuilder().setToken(token).build())
                       .subscribe(responseObserver::onNext,
                                  responseObserver::onError,
                                  responseObserver::onCompleted);
    }

    public void readHighestSequenceNr(ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> streamObserver) {
        CallStreamObserver<ReadHighestSequenceNrResponse> callStreamObserver = (CallStreamObserver<ReadHighestSequenceNrResponse>) streamObserver;
        ForwardingStreamObserver<ReadHighestSequenceNrResponse> responseObserver =
                new ForwardingStreamObserver<>(logger, "readHighestSequenceNr", callStreamObserver);

        eventDispatcher.highestSequenceNumber(contextProvider.getContext(), request.getAggregateId())
                       .map(l -> ReadHighestSequenceNrResponse.newBuilder().setToSequenceNr(l).build())
                       .subscribe(responseObserver::onNext,
                                  responseObserver::onError,
                                  responseObserver::onCompleted);
    }

    public StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> streamObserver) {
        return new StreamObserver<QueryEventsRequest>() {

            private final AtomicReference<Sinks.Many<QueryEventsRequest>> requestSinkRef = new AtomicReference<>();

            @Override
            public void onNext(QueryEventsRequest queryEventsRequest) {
                CallStreamObserver<QueryEventsResponse> callStreamObserver = (CallStreamObserver<QueryEventsResponse>) streamObserver;
                ForwardingStreamObserver<QueryEventsResponse> responseObserver =
                        new ForwardingStreamObserver<>(logger, "queryEvents", callStreamObserver);
                String contextName = queryEventsRequest.getContextName().isEmpty() ? contextProvider.getContext(): queryEventsRequest.getContextName();
                if (requestSinkRef.compareAndSet(null, Sinks.many().unicast().onBackpressureBuffer())) {
                    Authentication authentication = authenticationProvider.get();
                    if (authenticationProvider.get().isAuthenticated() && !axonServerAccessController.allowed(
                            EventStoreGrpc.getQueryEventsMethod().getFullMethodName(),
                            contextName,
                            authenticationProvider.get())) {
                        streamObserver.onError(GrpcExceptionBuilder.build(
                                new MessagingPlatformException(ErrorCode.AUTHENTICATION_INVALID_TOKEN,
                                                               "Query operation not allowed")));
                        return;
                    }
                    eventDispatcher.queryEvents(contextName, authentication, requestSinkRef.get().asFlux())
                                   .subscribe(responseObserver::onNext,
                                              responseObserver::onError,
                                              responseObserver::onCompleted);
                }
                Sinks.EmitResult emitResult = requestSinkRef.get().tryEmitNext(queryEventsRequest);
                if (emitResult.isFailure()) {
                    String message = String.format("%s: Error forwarding request to event store.", contextName);
                    logger.warn(message);
                    requestSinkRef.get().tryEmitError(new RuntimeException(message));
                }
            }

            @Override
            public void onError(Throwable reason) {
                if (!ExceptionUtils.isCancelled(reason)) {
                    logger.warn("Error on connection from client: {}", reason.getMessage());
                }
                Sinks.Many<QueryEventsRequest> sink = requestSinkRef.get();
                if (sink != null) {
                    sink.tryEmitError(reason);
                }
            }

            @Override
            public void onCompleted() {
                Sinks.Many<QueryEventsRequest> sink = requestSinkRef.get();
                if (sink != null) {
                    sink.tryEmitComplete();
                }
            }
        };
    }

    public void listAggregateSnapshots(GetAggregateSnapshotsRequest request,
                                       StreamObserver<SerializedEvent> responseObserver) {
        String context = contextProvider.getContext();
        Executor executor = grpcFlowControlExecutorProvider.provide();
        OutgoingStream<SerializedEvent> outgoingStream =
                new FlowControlledOutgoingStream<>((CallStreamObserver<SerializedEvent>) responseObserver,
                                                   executor);
        outgoingStream.accept(eventDispatcher.aggregateSnapshots(context, authenticationProvider.get(), request));
    }

}
