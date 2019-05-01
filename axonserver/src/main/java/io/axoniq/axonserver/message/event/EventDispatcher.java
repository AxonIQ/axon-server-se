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
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.*;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.util.ReadyStreamObserver;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.grpc.stub.ServerCalls.*;

/**
 * @author Marc Gathier
 */
@Component("EventDispatcher")
public class EventDispatcher implements AxonServerClientService {

    private static final String EVENTS_METRIC_NAME = "axon.events.count";
    private static final String SNAPSHOTS_METRIC_NAME = "axon.snapshots.count";
    private static final String NO_EVENT_STORE_CONFIGURED = "No event store available for: ";

    static final String ERROR_ON_CONNECTION_FROM_EVENT_STORE = "{}:  Error on connection from event store: {}";
    private final Logger logger = LoggerFactory.getLogger(EventDispatcher.class);
    public static final MethodDescriptor<GetEventsRequest, InputStream> METHOD_LIST_EVENTS =
            EventStoreGrpc.getListEventsMethod().toBuilder(
                    ProtoUtils.marshaller(GetEventsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller())
                                             .build();
    public static final MethodDescriptor<GetAggregateEventsRequest, InputStream> METHOD_LIST_AGGREGATE_EVENTS =
            EventStoreGrpc.getListAggregateEventsMethod().toBuilder(
                    ProtoUtils.marshaller(GetAggregateEventsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller())
                                              .build();

    public static final MethodDescriptor<GetAggregateSnapshotsRequest, InputStream> METHOD_LIST_AGGREGATE_SNAPSHOTS =
            EventStoreGrpc.getListAggregateSnapshotsMethod().toBuilder(
                    ProtoUtils.marshaller(GetAggregateSnapshotsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller())
                                                       .build();
    public static final MethodDescriptor<InputStream, Confirmation> METHOD_APPEND_EVENT =
            EventStoreGrpc.getAppendEventMethod().toBuilder(
                    InputStreamMarshaller.inputStreamMarshaller(), ProtoUtils.marshaller(Confirmation.getDefaultInstance()))
                          .build();

    private final EventStoreLocator eventStoreLocator;
    private final ContextProvider contextProvider;
    private final MetricCollector clusterMetrics;
    private final Map<ClientIdentification, List<EventTrackerInfo>> trackingEventProcessors = new ConcurrentHashMap<>();
    private final Counter eventsCounter;
    private final Counter snapshotCounter;

    public EventDispatcher(EventStoreLocator eventStoreLocator,
                           ContextProvider contextProvider,
                           MeterRegistry meterRegistry,
                           MetricCollector clusterMetrics) {
        this.contextProvider = contextProvider;
        this.clusterMetrics = clusterMetrics;
        this.eventStoreLocator = eventStoreLocator;
        eventsCounter = meterRegistry.counter(EVENTS_METRIC_NAME);
        snapshotCounter = meterRegistry.counter(SNAPSHOTS_METRIC_NAME);
    }


    public StreamObserver<InputStream> appendEvent(StreamObserver<Confirmation> responseObserver) {
        return appendEvent(contextProvider.getContext(), new ForwardingStreamObserver<>(logger, "appendEvent", responseObserver));
    }

    public StreamObserver<InputStream> appendEvent(String context, StreamObserver<Confirmation> responseObserver) {
        EventStore eventStore = eventStoreLocator.getEventStore(context);

        if (eventStore == null) {
            responseObserver.onError(new MessagingPlatformException(ErrorCode.NO_EVENTSTORE,
                                                                    NO_EVENT_STORE_CONFIGURED + context));
            return new NoOpStreamObserver<>();
        }
        StreamObserver<InputStream> appendEventConnection = eventStore.createAppendEventConnection(context,
                                                                                                         responseObserver);
        return new StreamObserver<InputStream>() {
            @Override
            public void onNext(InputStream event) {
                appendEventConnection.onNext(event);
                eventsCounter.increment();
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("Error on connection from client: {}", throwable.getMessage());
                appendEventConnection.onError(throwable);
            }

            @Override
            public void onCompleted() {
                appendEventConnection.onCompleted();
            }
        };
    }


    public void appendSnapshot(Event event, StreamObserver<Confirmation> confirmationStreamObserver) {
        appendSnapshot(contextProvider.getContext(), event, new ForwardingStreamObserver<>(logger, "appendSnapshot", confirmationStreamObserver));
    }

    public void appendSnapshot(String context, Event request, StreamObserver<Confirmation> responseObserver) {
        checkConnection(context, responseObserver).ifPresent(eventStore -> {
            snapshotCounter.increment();
            eventStore.appendSnapshot(context, request).whenComplete((c, t) -> {
                if (t != null) {
                    logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "appendSnapshot", t.getMessage());
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(c);
                    responseObserver.onCompleted();
                }
            });
        });
    }

    public void listAggregateEvents(GetAggregateEventsRequest request, StreamObserver<InputStream> responseObserver) {
        listAggregateEvents(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, "listAggregateEvents", responseObserver));
    }

    public void listAggregateEvents(String context, GetAggregateEventsRequest request, StreamObserver<InputStream> responseObserver) {
        checkConnection(context, responseObserver).ifPresent(eventStore -> {
            try {
                eventStore.listAggregateEvents(context, request, responseObserver);
            } catch (RuntimeException t) {
                logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listAggregateEvents", t.getMessage(), t);
                responseObserver.onError(GrpcExceptionBuilder.build(t));
            }
        });
    }

    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<InputStream> responseObserver) {
        return listEvents(contextProvider.getContext(), new ForwardingStreamObserver<>(logger, "listEvents", responseObserver));
    }

    public StreamObserver<GetEventsRequest> listEvents(String context, StreamObserver<InputStream> responseObserver) {
        EventStore eventStore = eventStoreLocator.getEventStore(context);
        if (eventStore == null) {
            responseObserver.onError(new MessagingPlatformException(ErrorCode.NO_EVENTSTORE,
                                                                    NO_EVENT_STORE_CONFIGURED + context));
            return new NoOpStreamObserver<>();
        }

        return new GetEventsRequestStreamObserver(responseObserver, eventStore, context);
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected applicationDisconnected) {
        List<EventTrackerInfo> eventsStreams = trackingEventProcessors.remove(applicationDisconnected.clientIdentification());
        logger.debug("application disconnected: {}, eventsStreams: {}", applicationDisconnected.getClient(), eventsStreams);

        if( eventsStreams != null) {
            eventsStreams.forEach(streamObserver -> {
                try {
                    streamObserver.responseObserver.onCompleted();
                } catch( Exception ex) {
                    logger.debug("Error while closing tracking event processor connection from {} - {}", applicationDisconnected.getClient(), ex.getMessage());
                }
            });
        }
    }


    public long getNrOfEvents() {
        return (long)eventsCounter.count() + new CompositeMetric(new io.axoniq.axonserver.metric.Metrics(EVENTS_METRIC_NAME, clusterMetrics)).size();
    }

    public long getNrOfSnapshots() {
        return (long)snapshotCounter.count() + new CompositeMetric(new io.axoniq.axonserver.metric.Metrics(SNAPSHOTS_METRIC_NAME, clusterMetrics)).size();
    }

    public Map<String, Iterable<Long>> eventTrackerStatus() {
        Map<String, Iterable<Long>> trackers = new HashMap<>();
        trackingEventProcessors.forEach((client, infos) -> {
            List<Long> status = infos.stream().map(EventTrackerInfo::getLastToken).collect(Collectors.toList());
            trackers.put(client.toString(), status);
        });
        return trackers;
    }


    @Override
    public final io.grpc.ServerServiceDefinition bindService() {
        return io.grpc.ServerServiceDefinition.builder(EventStoreGrpc.SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_APPEND_EVENT,
                                                      asyncClientStreamingCall( this::appendEvent))
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
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger, "getFirstToken", responseObserver0);
        checkConnection(contextProvider.getContext(), responseObserver).ifPresent(client ->
            client.getFirstToken(contextProvider.getContext(), request, responseObserver)
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
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger, "getLastToken", responseObserver0);
        checkConnection(contextProvider.getContext(), responseObserver).ifPresent(client ->
                                                            client.getLastToken(contextProvider.getContext(), request, responseObserver)
        );
    }

    public void getTokenAt(GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver0) {
        ForwardingStreamObserver<TrackingToken> responseObserver = new ForwardingStreamObserver<>(logger, "getTokenAt", responseObserver0);
        checkConnection(contextProvider.getContext(), responseObserver)
                .ifPresent(client ->client.getTokenAt(contextProvider.getContext(), request, responseObserver)
        );
    }

    public void readHighestSequenceNr(ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> responseObserver0) {
        ForwardingStreamObserver<ReadHighestSequenceNrResponse> responseObserver = new ForwardingStreamObserver<>(logger, "readHighestSequenceNr", responseObserver0);
        checkConnection(contextProvider.getContext(), responseObserver)
                .ifPresent(client -> client.readHighestSequenceNr(contextProvider.getContext(), request, responseObserver)
        );
    }


    public StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> responseObserver0) {
        ForwardingStreamObserver<QueryEventsResponse> responseObserver = new ForwardingStreamObserver<>(logger, "queryEvents", responseObserver0);
        return checkConnection(contextProvider.getContext(), responseObserver).map(client -> client.queryEvents(contextProvider.getContext(), responseObserver)).orElse(null);
    }

    public void listAggregateSnapshots(String context, GetAggregateSnapshotsRequest request,
                                       StreamObserver<InputStream> responseObserver) {
        checkConnection(context, responseObserver).ifPresent(eventStore -> {
            try {
                eventStore.listAggregateSnapshots(context, request, responseObserver);
            } catch (RuntimeException t) {
                logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listAggregateSnapshots", t.getMessage(), t);
                responseObserver.onError(GrpcExceptionBuilder.build(t));
            }
        });
    }

    private void listAggregateSnapshots(GetAggregateSnapshotsRequest request,
                                       StreamObserver<InputStream> responseObserver) {
        listAggregateSnapshots(contextProvider.getContext(), request, responseObserver);
    }



    private static class EventTrackerInfo {
        private final StreamObserver<InputStream> responseObserver;
        private final String client;
        private final String context;
        private final AtomicLong lastToken;

        public EventTrackerInfo(StreamObserver<InputStream> responseObserver, String client, String context, long lastToken) {
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
        private final EventStore eventStore;
        private final String context;
        volatile StreamObserver<GetEventsRequest> eventStoreRequestObserver;
        volatile EventTrackerInfo trackerInfo;

        GetEventsRequestStreamObserver(StreamObserver<InputStream> responseObserver, EventStore eventStore,
                                              String context) {
            this.responseObserver = responseObserver;
            this.eventStore = eventStore;
            this.context = context;
        }

        @Override
        public void onNext(GetEventsRequest getEventsRequest) {
            if (!registerEventTracker(getEventsRequest)) {
                return;
            }

            try {
                eventStoreRequestObserver.onNext(getEventsRequest);
            } catch (Exception reason ) {
                logger.warn("Error on connection sending event to client: {}", reason.getMessage());
                if( eventStoreRequestObserver != null ) eventStoreRequestObserver.onCompleted();
                removeTrackerInfo();
            }
        }

        private boolean registerEventTracker(GetEventsRequest getEventsRequest) {
            if( eventStoreRequestObserver == null) {
                trackerInfo = new EventTrackerInfo(responseObserver, getEventsRequest.getClientId(), context,getEventsRequest.getTrackingToken()-1);
                try {
                    eventStoreRequestObserver =
                            eventStore.listEvents(context, new ReadyStreamObserver<InputStream>() {
                                @Override
                                public boolean isReady() {
                                    if( responseObserver instanceof ForwardingStreamObserver) {
                                        return ((ForwardingStreamObserver<InputStream>) responseObserver).isReady();
                                    }
                                    return ((CallStreamObserver)responseObserver).isReady();
                                }

                                @Override
                                public void onNext(InputStream eventWithToken) {
                                    responseObserver.onNext(eventWithToken);
                                    trackerInfo.incrementLastToken();
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                    logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listEvents",
                                                throwable.getMessage());
                                    try {
                                        responseObserver.onError(GrpcExceptionBuilder.build(throwable));
                                    } catch( RuntimeException ex) {
                                        logger.info("Failed to send error: {}", ex.getMessage());
                                    }
                                    removeTrackerInfo();
                                }

                                @Override
                                public void onCompleted() {
                                    removeTrackerInfo();
                                    try {
                                        responseObserver.onCompleted();
                                    } catch (RuntimeException ignored) {
                                        // ignore error on confirming half close
                                    }
                                }
                            });
                } catch (RuntimeException cause) {
                    responseObserver.onError(GrpcExceptionBuilder.build(cause));
                    return false;
                }

                trackingEventProcessors.computeIfAbsent(new ClientIdentification(trackerInfo.context,trackerInfo.client),
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
            logger.warn("Error on connection from client: {}", reason.getMessage());
            if( eventStoreRequestObserver != null ) eventStoreRequestObserver.onCompleted();
            removeTrackerInfo();
        }

        private void removeTrackerInfo() {
            logger.info("Removed tracker info {}", trackerInfo);
            if (trackerInfo != null) {
                trackingEventProcessors.computeIfPresent(new ClientIdentification(trackerInfo.context,trackerInfo.client),
                                                         (c,streams) -> {
                                                                logger.debug("{}: {} streams", trackerInfo.client, streams.size());
                                                                streams.remove(trackerInfo);
                                                                return streams.isEmpty() ? null : streams;
                                                            });
            }
        }

        @Override
        public void onCompleted() {
            if( eventStoreRequestObserver != null ) eventStoreRequestObserver.onCompleted();
            removeTrackerInfo();
            responseObserver.onCompleted();
        }
    }
}
