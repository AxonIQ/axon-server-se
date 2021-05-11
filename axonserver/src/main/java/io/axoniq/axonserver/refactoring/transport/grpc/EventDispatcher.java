/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.ClientStreamIdentification;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.ExceptionUtils;
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
import io.axoniq.axonserver.refactoring.configuration.TopologyEvents;
import io.axoniq.axonserver.refactoring.messaging.MessagingPlatformException;
import io.axoniq.axonserver.refactoring.metric.BaseMetricName;
import io.axoniq.axonserver.refactoring.metric.MeterFactory;
import io.axoniq.axonserver.refactoring.requestprocessor.store.EventStoreService;
import io.axoniq.axonserver.refactoring.store.EventStoreLocator;
import io.axoniq.axonserver.refactoring.store.SerializedEvent;
import io.axoniq.axonserver.refactoring.store.api.*;
import io.axoniq.axonserver.refactoring.transport.ContextProvider;
import io.axoniq.axonserver.refactoring.transport.rest.SpringAuthentication;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.exception.ErrorCode.NO_EVENTSTORE;
import static io.grpc.stub.ServerCalls.*;

/**
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @author Stefan Dragisic
 */
@Component("EventDispatcher")
public class EventDispatcher implements AxonServerClientService {

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
    static final String ERROR_ON_CONNECTION_FROM_EVENT_STORE = "{}:  Error on connection from event store: {}";
    private static final String NO_EVENT_STORE_CONFIGURED = "No event store available for: ";
    private final Logger logger = LoggerFactory.getLogger(EventDispatcher.class);
    private final EventStoreLocator eventStoreLocator;
    private final AuthenticationProvider authenticationProvider;
    private final MeterFactory meterFactory;
    private final ContextProvider contextProvider;
    private final Map<ClientStreamIdentification, List<EventTrackerInfo>> trackingEventProcessors = new ConcurrentHashMap<>();
    private final Map<String, MeterFactory.RateMeter> eventsCounter = new ConcurrentHashMap<>();
    private final Map<String, MeterFactory.RateMeter> snapshotCounter = new ConcurrentHashMap<>();
    private final GrpcFlowControlExecutorProvider grpcFlowControlExecutorProvider;
    private final RetryBackoffSpec retrySpec;
    private final EventStoreService eventStoreService;

    public EventDispatcher(EventStoreLocator eventStoreLocator,
                           ContextProvider contextProvider,
                           AuthenticationProvider authenticationProvider,
                           MeterFactory meterFactory,
                           GrpcFlowControlExecutorProvider grpcFlowControlExecutorProvider,
                           @Value("${axoniq.axonserver.event.aggregate.retry.attempts:3}") int maxRetryAttempts,
                           @Value("${axoniq.axonserver.event.aggregate.retry.delay:100}") long retryDelayMillis, EventStoreService eventStoreService) {
        this.contextProvider = contextProvider;
        this.eventStoreLocator = eventStoreLocator;
        this.authenticationProvider = authenticationProvider;
        this.meterFactory = meterFactory;
        this.grpcFlowControlExecutorProvider = grpcFlowControlExecutorProvider;
        this.eventStoreService = eventStoreService;
        retrySpec = Retry.backoff(maxRetryAttempts, Duration.ofMillis(retryDelayMillis));
    }


    public StreamObserver<InputStream> appendEvent(StreamObserver<Confirmation> responseObserver) {
        AtomicReference<FluxSink<io.axoniq.axonserver.refactoring.store.api.Event>> eventSinkRef = new AtomicReference<>();
        StreamObserver<InputStream> inputStreamStreamObserver = new StreamObserver<InputStream>() {
            @Override
            public void onNext(InputStream inputStream) {
                eventSinkRef.get().next(null);//todo convert input stream to event
            }

            @Override
            public void onError(Throwable throwable) {
                eventSinkRef.get().error(throwable);
            }

            @Override
            public void onCompleted() {
                eventSinkRef.get().complete();
            }
        };
        // TODO: 5/11/21 add flow control magic
        eventStoreService.appendEvents(contextProvider.getContext(),
                Flux.create(eventSinkRef::set),
                new SpringAuthentication(authenticationProvider.get()))
        .subscribe(successConsumer -> {
            responseObserver.onNext(Confirmation.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }, responseObserver::onError);
        return inputStreamStreamObserver;
//        CallStreamObserver<Confirmation> callStreamObserver = (CallStreamObserver<Confirmation>) responseObserver;
//        return appendEvent(contextProvider.getContext(), authenticationProvider.get(),
//                           new ForwardingStreamObserver<>(logger, "appendEvent", callStreamObserver));
    }

//    public StreamObserver<InputStream> appendEvent(String context, Authentication authentication,
//                                                   StreamObserver<Confirmation> responseObserver) {
//        EventStore eventStore = eventStoreLocator.getEventStore(context);
//
//        if (eventStore == null) {
//            responseObserver.onError(new MessagingPlatformException(NO_EVENTSTORE,
//                                                                    NO_EVENT_STORE_CONFIGURED + context));
//            return new NoOpStreamObserver<>();
//        }
//        StreamObserver<InputStream> appendEventConnection =
//                eventStore.createAppendEventConnection(context, authentication,
//                                                       new StreamObserver<Confirmation>() {
//                                                           @Override
//                                                           public void onNext(Confirmation confirmation) {
//                                                               responseObserver.onNext(confirmation);
//                                                           }
//
//                                                           @Override
//                                                           public void onError(Throwable throwable) {
//                                                               StreamObserverUtils.error(responseObserver,
//                                                                                         MessagingPlatformException
//                                                                                                 .create(throwable));
//                                                           }
//
//                                                           @Override
//                                                           public void onCompleted() {
//
//                                                               responseObserver.onCompleted();
//                                                           }
//                                                       });
//        return new StreamObserver<InputStream>() {
//            @Override
//            public void onNext(InputStream inputStream) {
//                try {
//                    appendEventConnection.onNext(inputStream);
//                    eventsCounter(context, eventsCounter, BaseMetricName.AXON_EVENTS).mark();
//                } catch (Exception exception) {
//                    StreamObserverUtils.error(appendEventConnection, exception);
//                    StreamObserverUtils.error(responseObserver, MessagingPlatformException.create(exception));
//                }
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                logger.warn("Error on connection from client: {}", throwable.getMessage());
//                StreamObserverUtils.error(appendEventConnection, throwable);
//            }
//
//            @Override
//            public void onCompleted() {
//                appendEventConnection.onCompleted();
//            }
//        };
//    }

    private MeterFactory.RateMeter eventsCounter(String context, Map<String, MeterFactory.RateMeter> eventsCounter,
                                                 BaseMetricName eventsMetricName) {
        return eventsCounter.computeIfAbsent(context, c -> meterFactory.rateMeter(eventsMetricName,
                                                                                  Tags.of(MeterFactory.CONTEXT,
                                                                                          context)));
    }


    public void appendSnapshot(Event event, StreamObserver<Confirmation> streamObserver) {
        eventStoreService.appendSnapshot(contextProvider.getContext(), map(event), new SpringAuthentication(authenticationProvider.get()))
        .subscribe(successConsumer -> {
            streamObserver.onNext(Confirmation.newBuilder().setSuccess(true).build());
            streamObserver.onCompleted();
        }, streamObserver::onError);
//        CallStreamObserver<Confirmation> callStreamObserver = (CallStreamObserver<Confirmation>) streamObserver;
//        appendSnapshot(contextProvider.getContext(),
//                       authenticationProvider.get(),
//                       event,
//                       new ForwardingStreamObserver<>(logger, "appendSnapshot", callStreamObserver));
    }

    private Snapshot map(Event event) {
        // TODO: 5/11/21
        return null;
    }

//    public void appendSnapshot(String context, Authentication authentication, Event snapshot,
//                               StreamObserver<Confirmation> responseObserver) {
//        checkConnection(context, responseObserver).ifPresent(eventStore -> {
//            try {
//                eventsCounter(context, snapshotCounter, BaseMetricName.AXON_SNAPSHOTS).mark();
//                eventStore.appendSnapshot(context, authentication, snapshot).whenComplete((c, t) -> {
//                    if (t != null) {
//                        logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "appendSnapshot", t.getMessage());
//                        responseObserver.onError(t);
//                    } else {
//                        responseObserver.onNext(c);
//                        responseObserver.onCompleted();
//                    }
//                });
//            } catch (Exception ex) {
//                responseObserver.onError(ex);
//            }
//        });
//    }


    public void listAggregateEvents(GetAggregateEventsRequest request,
                                    StreamObserver<SerializedEvent> responseObserver) {
        // TODO: 5/11/21 flow control
        eventStoreService.aggregateEvents(map(request), new SpringAuthentication(authenticationProvider.get()))
                         .subscribe(event -> responseObserver.onNext(map(event)),
                                 responseObserver::onError,
                                 responseObserver::onCompleted);
//        CallStreamObserver<SerializedEvent> streamObserver = (CallStreamObserver<SerializedEvent>) responseObserver;
//        CallStreamObserver<SerializedEvent> validateStreamObserver = new SequenceValidationStreamObserver(streamObserver);
//        listAggregateEvents(contextProvider.getContext(), authenticationProvider.get(),
//                            request,
//                            new ForwardingStreamObserver<>(logger, "listAggregateEvents", validateStreamObserver));
    }

    private AggregateEventsQuery map(GetAggregateEventsRequest request) {
        return null;
    }

    private SerializedEvent map(io.axoniq.axonserver.refactoring.store.api.Event event) {
        return null;
    }

//    public void listAggregateEvents(String context, Authentication principal, GetAggregateEventsRequest request,
//                                    CallStreamObserver<SerializedEvent> responseObserver) {
//        final String LAST_SEQ_KEY = "__LAST_SEQ";
//        checkConnection(context, responseObserver).ifPresent(eventStore -> {
//            try {
//                Executor executor = grpcFlowControlExecutorProvider.provide();
//                OutgoingStream<SerializedEvent> outgoingStream = new FlowControlledOutgoingStream<>(responseObserver,
//                                                                                                    executor);
//                Flux<SerializedEvent> publisher;
//                publisher = Flux.deferContextual(contextView -> {
//                                                     AtomicLong lastSeq = contextView.get(LAST_SEQ_KEY);
//                                                     boolean allowedSnapshot =
//                                                             request.getAllowSnapshots()
//                                                                     && lastSeq.get() == -1;
//
//                                                     GetAggregateEventsRequest newRequest = request
//                                                             .toBuilder()
//                                                             .setAllowSnapshots(
//                                                                     allowedSnapshot)
//                                                             .setInitialSequence(lastSeq.get() + 1)
//                                                             .build();
//
//                                                     logger.debug("Reading events from seq#{} for aggregate {}",
//                                                                  lastSeq.get() + 1,
//                                                                  request.getAggregateId());
//                                                     return eventStore
//                                                             .aggregateEvents(context, principal, newRequest);
//                                                 }
//                )
//                                .doOnEach(signal -> {
//                                    if (signal.hasValue()) {
//                                        ((AtomicLong) signal.getContextView().get(LAST_SEQ_KEY))
//                                                .set(signal.get().getAggregateSequenceNumber());
//                                    }
//                                })
//                                .retryWhen(retrySpec
//                                                   .doBeforeRetry(t -> logger
//                                                           .warn("Retrying to read events aggregate stream due to {}:{}, for aggregate: {}",
//                                                                 t.failure().getClass().getName(),
//                                                                 t.failure().getMessage(),
//                                                                 request.getAggregateId())))
//                                .doOnError(t -> logger.error("Error during reading aggregate events. ", t))
//                                .doOnNext(m -> logger.trace("event {} for aggregate {}", m, request.getAggregateId()))
//                                .contextWrite(c -> c.put(LAST_SEQ_KEY, new AtomicLong(-1)))
//                                .name("event_stream")
//                                .tag("context", context)
//                                .tag("stream", "aggregate_events")
//                                .tag("origin", "cluster")
//                                .metrics();
//
//                outgoingStream.accept(publisher);
//            } catch (RuntimeException t) {
//                logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listAggregateEvents", t.getMessage(), t);
//                responseObserver.onError(GrpcExceptionBuilder.build(t));
//            }
//        });
//    }

//    public Flux<SerializedEvent> aggregateEvents(String context,
//                                                 Authentication principal,
//                                                 GetAggregateEventsRequest request) {
//        EventStore eventStore = eventStoreLocator.getEventStore(context);
//        if (eventStore == null) {
//            return Flux.error(new MessagingPlatformException(NO_EVENTSTORE, NO_EVENT_STORE_CONFIGURED + context));
//        }
//        return eventStore.aggregateEvents(context, principal, request);
//    }

    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<InputStream> responseObserver) {
        // TODO: 5/11/21 flow control
        SpringAuthentication springAuthentication = new SpringAuthentication(authenticationProvider.get());
        StreamObserver<GetEventsRequest> inputStream = new StreamObserver<GetEventsRequest>() {
            private final AtomicReference<Sinks.Many<PayloadType>> blackListSinkRef = new AtomicReference<>();
            private final AtomicReference<Disposable> eventSubscriptionRef = new AtomicReference<>();

            @Override
            public void onNext(GetEventsRequest getEventsRequest) {
                if (blackListSinkRef.compareAndSet(null, Sinks.many().unicast().onBackpressureBuffer())) {
                    // first message contains the request
                    Disposable eventSubscription = eventStoreService.events(map(getEventsRequest),
                            blackListSinkRef.get().asFlux(),
                            springAuthentication).subscribe(event -> responseObserver.onNext(map(event)),
                            responseObserver::onError,
                            responseObserver::onCompleted);
                } else {
                    // TODO: 5/11/21 check this
                    blackListSinkRef.get().tryEmitNext(mapBlackListUpdate(getEventsRequest)).orThrow();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                complete();
            }

            @Override
            public void onCompleted() {
                complete();
            }

            private void complete() {
                Disposable eventSubscription = eventSubscriptionRef.get();
                if (eventSubscription != null) {
                    eventSubscription.dispose();
                    blackListSinkRef.get().tryEmitComplete().orThrow();
                }
            }
        };

        return listEvents(contextProvider.getContext(), authenticationProvider.get(), responseObserver);
    }

    private EventsQuery map(GetEventsRequest request) {
        return null;
    }

    private PayloadType mapBlackListUpdate(GetEventsRequest request) {
        return null;
    }

    private InputStream map(EventWithToken event) {
        return null;
    }

    public StreamObserver<GetEventsRequest> listEvents(String context, Authentication principal,
                                                       StreamObserver<InputStream> responseObserver) {
        return new GetEventsRequestStreamObserver(responseObserver, context, principal);
    }

    // TODO: 5/11/21 think of moving this responsibility
    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected applicationDisconnected) {
        List<EventTrackerInfo> eventsStreams = trackingEventProcessors.remove(applicationDisconnected
                                                                                      .clientIdentification());
        logger.debug("application disconnected: {}, eventsStreams: {}",
                     applicationDisconnected.getClientStreamId(),
                     eventsStreams);

        if (eventsStreams != null) {
            eventsStreams.forEach(streamObserver -> {
                try {
                    streamObserver.responseObserver.onCompleted();
                } catch (Exception ex) {
                    logger.debug("Error while closing tracking event processor connection from {} - {}",
                                 applicationDisconnected.getClientStreamId(),
                                 ex.getMessage());
                }
            });
        }
    }

    public Map<String, Iterable<Long>> eventTrackerStatus(String context) {
        Map<String, Iterable<Long>> trackers = new HashMap<>();
        trackingEventProcessors.forEach((client, infos) -> {
            if (client.getContext().equals(context)) {
                List<Long> status = infos.stream().map(EventTrackerInfo::getLastToken).collect(Collectors.toList());
                trackers.put(client.toString(), status);
            }
        });
        return trackers;
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
        eventStoreService.firstEventToken(contextProvider.getContext(),
                new SpringAuthentication(authenticationProvider.get()))
        .subscribe(firstToken -> streamObserver.onNext(TrackingToken.newBuilder().setToken(firstToken).build()),
                streamObserver::onError, streamObserver::onCompleted);
//        CallStreamObserver<TrackingToken> callStreamObserver = (CallStreamObserver<TrackingToken>) streamObserver;
//        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
//                                                                                                  "getFirstToken",
//                                                                                                  callStreamObserver);
//        checkConnection(contextProvider.getContext(), responseObserver).ifPresent(client ->
//                                                                                          client.getFirstToken(
//                                                                                                  contextProvider
//                                                                                                          .getContext(),
//                                                                                                  request,
//                                                                                                  responseObserver)
//        );
    }

    private Optional<EventStore> checkConnection(String context, StreamObserver<?> responseObserver) {
        EventStore eventStore = eventStoreLocator.getEventStore(context);
        if (eventStore == null) {
            responseObserver.onError(new MessagingPlatformException(NO_EVENTSTORE,
                                                                    NO_EVENT_STORE_CONFIGURED + context));
            return Optional.empty();
        }
        return Optional.of(eventStore);
    }

    public void getLastToken(GetLastTokenRequest request, StreamObserver<TrackingToken> streamObserver) {
        eventStoreService.lastEventToken(contextProvider.getContext(),
                new SpringAuthentication(authenticationProvider.get()))
                .subscribe(lastToken -> streamObserver.onNext(TrackingToken.newBuilder().setToken(lastToken).build()),
                        streamObserver::onError, streamObserver::onCompleted);
//        CallStreamObserver<TrackingToken> callStreamObserver = (CallStreamObserver<TrackingToken>) streamObserver;
//        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
//                                                                                                  "getLastToken",
//                                                                                                  callStreamObserver);
//        checkConnection(contextProvider.getContext(), responseObserver).ifPresent(client ->
//                                                                                          client.getLastToken(
//                                                                                                  contextProvider
//                                                                                                          .getContext(),
//                                                                                                  request,
//                                                                                                  responseObserver)
//        );
    }

    public void getTokenAt(GetTokenAtRequest request, StreamObserver<TrackingToken> streamObserver) {
        eventStoreService.eventTokenAt(contextProvider.getContext(), Instant.ofEpochMilli(request.getInstant()),
                new SpringAuthentication(authenticationProvider.get()))
                .subscribe(token -> streamObserver.onNext(TrackingToken.newBuilder().setToken(token).build()),
                        streamObserver::onError, streamObserver::onCompleted);
//        CallStreamObserver<TrackingToken> callStreamObserver = (CallStreamObserver<TrackingToken>) streamObserver;
//        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
//                                                                                                  "getTokenAt",
//                                                                                                  callStreamObserver);
//        checkConnection(contextProvider.getContext(), responseObserver)
//                .ifPresent(client -> client.getTokenAt(contextProvider.getContext(), request, responseObserver)
//                );
    }

    public void readHighestSequenceNr(ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> streamObserver) {
        eventStoreService.highestSequenceNumber(contextProvider.getContext(), request.getAggregateId(),
                new SpringAuthentication(authenticationProvider.get()))
                .subscribe(seqNumber -> streamObserver.onNext(ReadHighestSequenceNrResponse.newBuilder()
                                .setToSequenceNr(seqNumber).build()),
                        streamObserver::onError, streamObserver::onCompleted);
//        CallStreamObserver<ReadHighestSequenceNrResponse> callStreamObserver = (CallStreamObserver<ReadHighestSequenceNrResponse>) streamObserver;
//        ForwardingStreamObserver<ReadHighestSequenceNrResponse> responseObserver =
//                new ForwardingStreamObserver<>(logger, "readHighestSequenceNr", callStreamObserver);
//        checkConnection(contextProvider.getContext(), responseObserver)
//                .ifPresent(client -> client
//                        .readHighestSequenceNr(contextProvider.getContext(), request, responseObserver)
//                );
    }

    private AdHocEventsQuery mapEvent(QueryEventsRequest request) {
        return null;
    }

    private AdHocSnapshotsQuery mapSnapshot(QueryEventsRequest request) {
        return null;
    }

    private QueryEventsResponse mapEvent(EventQueryResponse response) {
        return null;
    }

    private QueryEventsResponse mapSnapshot(SnapshotQueryResponse response) {
        return null;
    }

    public StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> streamObserver) {

        StreamObserver<QueryEventsRequest> inputStream = new StreamObserver<QueryEventsRequest>() {

            private final AtomicReference<Disposable> subscriptionRef = new AtomicReference<>();

            @Override
            public void onNext(QueryEventsRequest queryEventsRequest) {
                if (subscriptionRef.get() == null) {
                    Disposable subscription;
                    if (queryEventsRequest.getQuerySnapshots()) {
                        subscription = eventStoreService.querySnapshots(mapSnapshot(queryEventsRequest),
                                new SpringAuthentication(authenticationProvider.get()))
                                .subscribe(event -> streamObserver.onNext(mapSnapshot(event)),
                                        streamObserver::onError,
                                        streamObserver::onCompleted);
                    } else {
                        subscription = eventStoreService.queryEvents(mapEvent(queryEventsRequest),
                                new SpringAuthentication(authenticationProvider.get()))
                                .subscribe(event -> streamObserver.onNext(mapEvent(event)),
                                        streamObserver::onError,
                                        streamObserver::onCompleted);
                    }
                    subscriptionRef.set(subscription);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                complete();
            }

            @Override
            public void onCompleted() {
                complete();
            }

            private void complete() {
                Disposable subscription = subscriptionRef.get();
                if (subscription != null) {
                    subscription.dispose();
                }
            }
        };

        return inputStream;

//        CallStreamObserver<QueryEventsResponse> callStreamObserver = (CallStreamObserver<QueryEventsResponse>) streamObserver;
//        String context = contextProvider.getContext();
//        Authentication authentication = authenticationProvider.get();
//        ForwardingStreamObserver<QueryEventsResponse> responseObserver =
//                new ForwardingStreamObserver<>(logger, "queryEvents", callStreamObserver);
//        return new StreamObserver<QueryEventsRequest>() {
//
//            private final AtomicReference<StreamObserver<QueryEventsRequest>> requestObserver = new AtomicReference<>();
//
//            @Override
//            public void onNext(QueryEventsRequest request) {
//                if (requestObserver.get() == null) {
//                    EventStore eventStore = eventStoreLocator.getEventStore(context,
//                                                                            request.getForceReadFromLeader());
//                    if (eventStore == null) {
//                        responseObserver.onError(new MessagingPlatformException(NO_EVENTSTORE,
//                                                                                NO_EVENT_STORE_CONFIGURED + context));
//                        return;
//                    }
//                    requestObserver.set(eventStore.queryEvents(context, authentication, responseObserver));
//                }
//                try {
//                    requestObserver.get().onNext(request);
//                } catch (Exception reason) {
//                    logger.warn("{}: Error forwarding request to event store: {}", context, reason.getMessage());
//                    StreamObserverUtils.complete(requestObserver.get());
//                }
//            }
//
//            @Override
//            public void onError(Throwable reason) {
//                if (!ExceptionUtils.isCancelled(reason)) {
//                    logger.warn("Error on connection from client: {}", reason.getMessage());
//                }
//                cleanup();
//            }
//
//            @Override
//            public void onCompleted() {
//                cleanup();
//                StreamObserverUtils.complete(responseObserver);
//            }
//
//            private void cleanup() {
//                StreamObserverUtils.complete(requestObserver.get());
//            }
//        };
    }

    public void listAggregateSnapshots(String context, Authentication authentication,
                                       GetAggregateSnapshotsRequest request,
                                       StreamObserver<SerializedEvent> responseObserver) {
        checkConnection(context, responseObserver).ifPresent(eventStore -> {
            try {
                eventStore.listAggregateSnapshots(context,
                                                  authentication,
                                                  request,
                                                  responseObserver);
            } catch (RuntimeException t) {
                logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listAggregateSnapshots", t.getMessage(), t);
                responseObserver.onError(GrpcExceptionBuilder.build(t));
            }
        });
    }

    public void listAggregateSnapshots(GetAggregateSnapshotsRequest request,
                                       StreamObserver<SerializedEvent> responseObserver) {

        eventStoreService.aggregateSnapshots(map(request), new SpringAuthentication(authenticationProvider.get()))
        .subscribe(snapshot -> responseObserver.onNext(map(snapshot)), responseObserver::onError, responseObserver::onCompleted);
//        listAggregateSnapshots(contextProvider.getContext(), authenticationProvider.get(), request, responseObserver);
    }

    private AggregateSnapshotsQuery map(GetAggregateSnapshotsRequest request) {
        return null;
    }

    private SerializedEvent map(Snapshot snapshot) {
        return null;
    }

    public MeterFactory.RateMeter eventRate(String context) {
        return eventsCounter(context, eventsCounter, BaseMetricName.AXON_EVENTS);
    }

    public MeterFactory.RateMeter snapshotRate(String context) {
        return eventsCounter(context, snapshotCounter, BaseMetricName.AXON_SNAPSHOTS);
    }

    private static class EventTrackerInfo {

        private final StreamObserver<InputStream> responseObserver;
        private final String client;
        private final String context;
        private final AtomicLong lastToken;

        public EventTrackerInfo(StreamObserver<InputStream> responseObserver, String client, String context,
                                long lastToken) {
            this.responseObserver = responseObserver;
            this.client = client;
            this.context = context;
            this.lastToken = new AtomicLong(lastToken);
        }

        public StreamObserver<InputStream> getResponseObserver() {
            return responseObserver;
        }

        public String getClient() {
            return client;
        }

        public long getLastToken() {
            return lastToken.get();
        }

        public String getContext() {
            return context;
        }

        void incrementLastToken() {
            lastToken.incrementAndGet();
        }

        @Override
        public String toString() {
            return "EventTrackerInfo{" +
                    "responseObserver=" + responseObserver +
                    ", client='" + client + '\'' +
                    ", context='" + context + '\'' +
                    ", lastToken=" + lastToken +
                    '}';
        }
    }

    private class GetEventsRequestStreamObserver implements StreamObserver<GetEventsRequest> {

        private final StreamObserver<InputStream> responseObserver;
        private final Authentication principal;
        private final String context;
        volatile StreamObserver<GetEventsRequest> eventStoreRequestObserver;
        volatile EventTrackerInfo trackerInfo;

        GetEventsRequestStreamObserver(StreamObserver<InputStream> responseObserver, String context,
                                       Authentication principal) {
            this.context = context;
            this.responseObserver = responseObserver;
            this.principal = principal;
        }

        @Override
        public void onNext(GetEventsRequest getEventsRequest) {
            if (!registerEventTracker(getEventsRequest)) {
                return;
            }

            try {
                eventStoreRequestObserver.onNext(getEventsRequest);
            } catch (Exception reason) {
                logger.warn("Error on connection sending event to client: {}", reason.getMessage());
                if (eventStoreRequestObserver != null) {
                    eventStoreRequestObserver.onCompleted();
                }
                removeTrackerInfo();
            }
        }

        private boolean registerEventTracker(GetEventsRequest getEventsRequest) {
            if (eventStoreRequestObserver == null) {
                trackerInfo = new EventTrackerInfo(responseObserver,
                                                   getEventsRequest.getClientId(),
                                                   context,
                                                   getEventsRequest.getTrackingToken() - 1);
                try {
                    EventStore eventStore = eventStoreLocator
                            .getEventStore(context,
                                           getEventsRequest.getForceReadFromLeader());
                    if (eventStore == null) {
                        responseObserver.onError(new MessagingPlatformException(NO_EVENTSTORE,
                                                                                NO_EVENT_STORE_CONFIGURED + context));
                        return false;
                    }
                    eventStoreRequestObserver =
                            eventStore.listEvents(context, principal, new StreamObserver<InputStream>() {
                                @Override
                                public void onNext(InputStream eventWithToken) {
                                    responseObserver.onNext(eventWithToken);
                                    trackerInfo.incrementLastToken();
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                    if (throwable instanceof IllegalStateException) {
                                        logger.debug(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listEvents",
                                                     throwable.getMessage());
                                    } else {
                                        logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listEvents",
                                                    throwable.getMessage());
                                    }
                                    StreamObserverUtils.error(responseObserver, GrpcExceptionBuilder.build(throwable));
                                    removeTrackerInfo();
                                }

                                @Override
                                public void onCompleted() {
                                    logger.info("{}: Tracking event processor closed", trackerInfo.context);
                                    removeTrackerInfo();
                                    StreamObserverUtils.complete(responseObserver);
                                }
                            });
                } catch (RuntimeException cause) {
                    responseObserver.onError(GrpcExceptionBuilder.build(cause));
                    return false;
                }

                trackingEventProcessors.computeIfAbsent(new ClientStreamIdentification(trackerInfo.context,
                                                                                       trackerInfo.client),
                                                        key -> new CopyOnWriteArrayList<>()).add(trackerInfo);
                logger.info("Starting tracking event processor for {}:{} - {}",
                            getEventsRequest.getClientId(),
                            getEventsRequest.getComponentName(),
                            getEventsRequest.getTrackingToken());
            }
            return true;
        }

        @Override
        public void onError(Throwable reason) {
            if (!ExceptionUtils.isCancelled(reason)) {
                logger.warn("Error on connection from client: {}", reason.getMessage());
            }
            cleanup();
        }

        private void removeTrackerInfo() {
            logger.info("Removed tracker info {}", trackerInfo);
            if (trackerInfo != null) {
                trackingEventProcessors.computeIfPresent(new ClientStreamIdentification(trackerInfo.context,
                                                                                        trackerInfo.client),
                                                         (c, streams) -> {
                                                             logger.debug("{}: {} streams",
                                                                          trackerInfo.client,
                                                                          streams.size());
                                                             streams.remove(trackerInfo);
                                                             return streams.isEmpty() ? null : streams;
                                                         });
            }
        }

        @Override
        public void onCompleted() {
            cleanup();
            StreamObserverUtils.complete(responseObserver);
        }

        private void cleanup() {
            StreamObserverUtils.complete(eventStoreRequestObserver);
            removeTrackerInfo();
        }
    }
}
