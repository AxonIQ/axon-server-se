/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ExceptionUtils;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
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
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.grpc.stub.ServerCalls.*;

/**
 * @author Marc Gathier
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

    public EventDispatcher(EventStoreLocator eventStoreLocator,
                           ContextProvider contextProvider,
                           AuthenticationProvider authenticationProvider,
                           MeterFactory meterFactory) {
        this.contextProvider = contextProvider;
        this.eventStoreLocator = eventStoreLocator;
        this.authenticationProvider = authenticationProvider;
        this.meterFactory = meterFactory;
    }


    public StreamObserver<InputStream> appendEvent(StreamObserver<Confirmation> responseObserver) {
        return appendEvent(contextProvider.getContext(), authenticationProvider.get(),
                           new ForwardingStreamObserver<>(logger, "appendEvent", responseObserver));
    }

    public StreamObserver<InputStream> appendEvent(String context, Authentication authentication,
                                                   StreamObserver<Confirmation> responseObserver) {
        EventStore eventStore = eventStoreLocator.getEventStore(context);

        if (eventStore == null) {
            responseObserver.onError(new MessagingPlatformException(ErrorCode.NO_EVENTSTORE,
                                                                    NO_EVENT_STORE_CONFIGURED + context));
            return new NoOpStreamObserver<>();
        }
        StreamObserver<InputStream> appendEventConnection =
                eventStore.createAppendEventConnection(context, authentication,
                                                       new StreamObserver<Confirmation>() {
                                                           @Override
                                                           public void onNext(Confirmation confirmation) {
                                                               responseObserver.onNext(confirmation);
                                                           }

                                                           @Override
                                                           public void onError(Throwable throwable) {
                                                               StreamObserverUtils.error(responseObserver,
                                                                                         MessagingPlatformException
                                                                                                     .create(throwable));
                                                           }

                                                           @Override
                                                           public void onCompleted() {

                                                                   responseObserver.onCompleted();
                                                           }
                                                       });
        return new StreamObserver<InputStream>() {
            @Override
            public void onNext(InputStream inputStream) {
                try {
                    appendEventConnection.onNext(inputStream);
                    eventsCounter(context, eventsCounter, BaseMetricName.AXON_EVENTS).mark();
                } catch (Exception exception) {
                    StreamObserverUtils.error(appendEventConnection, exception);
                    StreamObserverUtils.error(responseObserver, MessagingPlatformException.create(exception));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("Error on connection from client: {}", throwable.getMessage());
                StreamObserverUtils.error(appendEventConnection, throwable);
            }

            @Override
            public void onCompleted() {
                appendEventConnection.onCompleted();
            }
        };
    }

    private MeterFactory.RateMeter eventsCounter(String context, Map<String, MeterFactory.RateMeter> eventsCounter,
                                                 BaseMetricName eventsMetricName) {
        return eventsCounter.computeIfAbsent(context, c -> meterFactory.rateMeter(eventsMetricName,
                                                                                  Tags.of(MeterFactory.CONTEXT,
                                                                                          context)));
    }


    public void appendSnapshot(Event event, StreamObserver<Confirmation> confirmationStreamObserver) {
        appendSnapshot(contextProvider.getContext(),
                       authenticationProvider.get(),
                       event,
                       new ForwardingStreamObserver<>(logger, "appendSnapshot", confirmationStreamObserver));
    }

    public void appendSnapshot(String context, Authentication authentication, Event snapshot,
                               StreamObserver<Confirmation> responseObserver) {
        checkConnection(context, responseObserver).ifPresent(eventStore -> {
            try {
                eventsCounter(context, snapshotCounter, BaseMetricName.AXON_SNAPSHOTS).mark();
                eventStore.appendSnapshot(context, authentication, snapshot).whenComplete((c, t) -> {
                    if (t != null) {
                        logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "appendSnapshot", t.getMessage());
                        responseObserver.onError(t);
                    } else {
                        responseObserver.onNext(c);
                        responseObserver.onCompleted();
                    }
                });
            } catch (Exception ex) {
                responseObserver.onError(ex);
            }
        });
    }

    public void listAggregateEvents(GetAggregateEventsRequest request,
                                    StreamObserver<SerializedEvent> responseObserver) {
        StreamObserver<SerializedEvent> aggregateStreamObserver = new SequenceValidationStreamObserver(responseObserver);
        listAggregateEvents(contextProvider.getContext(), authenticationProvider.get(),
                            request,
                            new ForwardingStreamObserver<>(logger, "listAggregateEvents", aggregateStreamObserver));
    }

    public void listAggregateEvents(String context, Authentication principal, GetAggregateEventsRequest request,
                                    StreamObserver<SerializedEvent> responseObserver) {
        checkConnection(context, responseObserver).ifPresent(eventStore -> {
            try {
                eventStore.listAggregateEvents(context,
                                               principal,
                                               request,
                                               responseObserver);
            } catch (RuntimeException t) {
                logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listAggregateEvents", t.getMessage(), t);
                responseObserver.onError(GrpcExceptionBuilder.build(t));
            }
        });
    }

    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<InputStream> responseObserver) {
        return listEvents(contextProvider.getContext(), authenticationProvider.get(), responseObserver);
    }

    public StreamObserver<GetEventsRequest> listEvents(String context, Authentication principal,
                                                       StreamObserver<InputStream> responseObserver) {
        return new GetEventsRequestStreamObserver(responseObserver, context, principal);
    }

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


    public long getNrOfEvents(String context) {
        CompletableFuture<Long> lastTokenFuture = new CompletableFuture<>();
        try {
            eventStoreLocator.getEventStore(context).getLastToken(context,
                                                                  GetLastTokenRequest.newBuilder().build(),
                                                                  new StreamObserver<TrackingToken>() {
                                                                      @Override
                                                                      public void onNext(TrackingToken trackingToken) {
                                                                          lastTokenFuture.complete(trackingToken
                                                                                                           .getToken());
                                                                      }

                                                                      @Override
                                                                      public void onError(Throwable throwable) {
                                                                          lastTokenFuture.completeExceptionally(
                                                                                  throwable);
                                                                      }

                                                                      @Override
                                                                      public void onCompleted() {
                                                                          // no action needed
                                                                      }
                                                                  });


            return lastTokenFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        } catch (Exception e) {
            return -1;
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

    public void getFirstToken(GetFirstTokenRequest request, StreamObserver<TrackingToken> responseObserver0) {
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
                                                                                                  "getFirstToken",
                                                                                                  responseObserver0);
        checkConnection(contextProvider.getContext(), responseObserver).ifPresent(client ->
                                                                                          client.getFirstToken(
                                                                                                  contextProvider
                                                                                                          .getContext(),
                                                                                                  request,
                                                                                                  responseObserver)
        );
    }

    private Optional<EventStore> checkConnection(String context, StreamObserver<?> responseObserver) {
        EventStore eventStore = eventStoreLocator.getEventStore(context);
        if (eventStore == null) {
            responseObserver.onError(new MessagingPlatformException(ErrorCode.NO_EVENTSTORE,
                                                                    NO_EVENT_STORE_CONFIGURED + context));
            return Optional.empty();
        }
        return Optional.of(eventStore);
    }

    public void getLastToken(GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver0) {
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
                                                                                                  "getLastToken",
                                                                                                  responseObserver0);
        checkConnection(contextProvider.getContext(), responseObserver).ifPresent(client ->
                                                                                          client.getLastToken(
                                                                                                  contextProvider
                                                                                                          .getContext(),
                                                                                                  request,
                                                                                                  responseObserver)
        );
    }

    public void getTokenAt(GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver0) {
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger,
                                                                                                  "getTokenAt",
                                                                                                  responseObserver0);
        checkConnection(contextProvider.getContext(), responseObserver)
                .ifPresent(client -> client.getTokenAt(contextProvider.getContext(), request, responseObserver)
                );
    }

    public void readHighestSequenceNr(ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> responseObserver0) {
        ForwardingStreamObserver<ReadHighestSequenceNrResponse> responseObserver = new ForwardingStreamObserver<>(logger,
                                                                                                                  "readHighestSequenceNr",
                                                                                                                  responseObserver0);
        checkConnection(contextProvider.getContext(), responseObserver)
                .ifPresent(client -> client
                        .readHighestSequenceNr(contextProvider.getContext(), request, responseObserver)
                );
    }

    public StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> responseObserver0) {
        String context = contextProvider.getContext();
        Authentication authentication = authenticationProvider.get();
        ForwardingStreamObserver<QueryEventsResponse> responseObserver =
                new ForwardingStreamObserver<>(logger, "queryEvents", responseObserver0);
        return new StreamObserver<QueryEventsRequest>() {

            private final AtomicReference<StreamObserver<QueryEventsRequest>> requestObserver = new AtomicReference<>();

            @Override
            public void onNext(QueryEventsRequest request) {
                if (requestObserver.get() == null) {
                    EventStore eventStore = eventStoreLocator.getEventStore(context,
                                                                            request.getForceReadFromLeader());
                    if (eventStore == null) {
                        responseObserver.onError(new MessagingPlatformException(ErrorCode.NO_EVENTSTORE,
                                                                                NO_EVENT_STORE_CONFIGURED + context));
                        return;
                    }
                    requestObserver.set(eventStore.queryEvents(context, authentication, responseObserver));
                }
                try {
                    requestObserver.get().onNext(request);
                } catch (Exception reason) {
                    logger.warn("{}: Error forwarding request to event store: {}", context, reason.getMessage());
                    StreamObserverUtils.complete(requestObserver.get());
                }
            }

            @Override
            public void onError(Throwable reason) {
                if (!ExceptionUtils.isCancelled(reason)) {
                    logger.warn("Error on connection from client: {}", reason.getMessage());
                }
                cleanup();
            }

            @Override
            public void onCompleted() {
                cleanup();
                StreamObserverUtils.complete(responseObserver);
            }

            private void cleanup() {
                StreamObserverUtils.complete(requestObserver.get());
            }
        };
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
        listAggregateSnapshots(contextProvider.getContext(), authenticationProvider.get(), request, responseObserver);
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
                        responseObserver.onError(new MessagingPlatformException(ErrorCode.NO_EVENTSTORE,
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
