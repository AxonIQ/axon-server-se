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
import io.axoniq.axonserver.exception.ExceptionUtils;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

/**
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @author Stefan Dragisic
 */
@Component
public class EventDispatcher {

    private static final Logger auditLog = AuditLog.getLogger();

    static final String ERROR_ON_CONNECTION_FROM_EVENT_STORE = "{}:  Error on connection from event store: {}";
    private static final String NO_EVENT_STORE_CONFIGURED = "No event store available for: ";
    private final Logger logger = LoggerFactory.getLogger(EventDispatcher.class);
    private final EventStoreLocator eventStoreLocator;
    private final MeterFactory meterFactory;
    private final Map<ClientStreamIdentification, List<EventTrackerInfo>> trackingEventProcessors = new ConcurrentHashMap<>();
    private final Map<String, MeterFactory.RateMeter> eventsCounter = new ConcurrentHashMap<>();
    private final Map<String, MeterFactory.RateMeter> snapshotCounter = new ConcurrentHashMap<>();
    @Value("${axoniq.axonserver.read-sequence-validation-strategy:LOG}")
    private SequenceValidationStrategy sequenceValidationStrategy = SequenceValidationStrategy.LOG;
    private final RetryBackoffSpec retrySpec;
    private final int aggregateEventsPrefetch;

    public EventDispatcher(EventStoreLocator eventStoreLocator,
                           MeterFactory meterFactory,
                           @Value("${axoniq.axonserver.event.aggregate.retry.attempts:3}") int maxRetryAttempts,
                           @Value("${axoniq.axonserver.event.aggregate.retry.delay:100}") long retryDelayMillis,
                           @Value("${axoniq.axonserver.event.aggregate.prefetch:5}") int aggregateEventsPrefetch) {
        this.eventStoreLocator = eventStoreLocator;
        this.meterFactory = meterFactory;
        retrySpec = Retry.backoff(maxRetryAttempts, Duration.ofMillis(retryDelayMillis));
        this.aggregateEventsPrefetch = aggregateEventsPrefetch;
    }


    public Mono<Void> appendEvent(String context,
                                  Authentication authentication,
                                  Flux<SerializedEvent> eventFlux) {
        if (auditLog.isDebugEnabled()) {
            auditLog.debug("[{}@{}] Request to append events.", AuditLog.username(authentication), context);
        }

        return eventStoreLocator.eventStore(context).flatMap(eventStore ->  {
                                                                Flux<SerializedEvent> countingFlux =
                                                                        eventFlux.doOnNext(event ->  eventsCounter(context,
                                                                                                                   eventsCounter,
                                                                                                                   BaseMetricName.AXON_EVENTS).mark());
                                                                return eventStore.appendEvents(context,countingFlux,authentication);
                                                         });
    }

    private MeterFactory.RateMeter eventsCounter(String context, Map<String, MeterFactory.RateMeter> eventsCounter,
                                                 BaseMetricName eventsMetricName) {
        return eventsCounter.computeIfAbsent(context, c -> meterFactory.rateMeter(eventsMetricName,
                                                                                  Tags.of(MeterFactory.CONTEXT,
                                                                                          context)));
    }

    public Mono<Void> appendSnapshot(String context, Event snapshot, Authentication authentication) {
        if (auditLog.isDebugEnabled()) {
            auditLog.debug("[{}@{}] Request to list events for {}.",
                           AuditLog.username(authentication),
                           context,
                           snapshot.getAggregateIdentifier());
        }
        return eventStoreLocator.eventStore(context)
                                .flatMap(eventStore -> eventStore.appendSnapshot(context, snapshot, authentication)
                                                                 .doOnSuccess(v -> eventsCounter(context,
                                                                                                 snapshotCounter,
                                                                                                 BaseMetricName.AXON_SNAPSHOTS).mark())
                                                                 .doOnError(t -> logger.warn(
                                                                         ERROR_ON_CONNECTION_FROM_EVENT_STORE,
                                                                         "appendSnapshot",
                                                                         t.getMessage())));
    }

    public Flux<SerializedEvent> aggregateEvents(String context,
                                                 Authentication authentication,
                                                 GetAggregateEventsRequest request) {
        if( auditLog.isDebugEnabled()) {
            auditLog.debug("[{}@{}] Request to list events for {}.", AuditLog.username(authentication), context, request.getAggregateId());
        }

        return eventStoreLocator.eventStore(context)
                         .flatMapMany(eventStore -> {
                             final String LAST_SEQ_KEY = "__LAST_SEQ";
                             return Flux.deferContextual(contextView -> {
                                                             AtomicLong lastSeq = contextView.get(LAST_SEQ_KEY);
                                                             boolean allowedSnapshot =
                                                                     request.getAllowSnapshots()
                                                                             && lastSeq.get() == -1;

                                                             GetAggregateEventsRequest newRequest = request
                                                                     .toBuilder()
                                                                     .setAllowSnapshots(allowedSnapshot)
                                                                     .setInitialSequence(lastSeq.get() + 1)
                                                                     .build();

                                                             logger.debug("Reading events from seq#{} for aggregate {}",
                                                                          lastSeq.get() + 1,
                                                                          request.getAggregateId());
                                                             return eventStore
                                                                     .aggregateEvents(context, authentication, newRequest);
                                                         }
                                        )
                                        .limitRate(aggregateEventsPrefetch * 5, aggregateEventsPrefetch)
                                        .doOnEach(signal -> {
                                            if (signal.hasValue()) {
                                                ((AtomicLong) signal.getContextView().get(LAST_SEQ_KEY))
                                                        .set(signal.get().getAggregateSequenceNumber());
                                            }
                                        })
                                        .retryWhen(retrySpec
                                                           .doBeforeRetry(t -> logger.warn(
                                                                   "Retrying to read events aggregate stream due to {}:{}, for aggregate: {}",
                                                                   t.failure().getClass().getName(),
                                                                   t.failure().getMessage(),
                                                                   request.getAggregateId())))
                                     .onErrorMap(ex -> {
                                         if (Exceptions.isRetryExhausted(ex)) {
                                             return ex.getCause();
                                         }
                                         return ex;
                                     })
                                        .doOnError(t -> logger.error("Error during reading aggregate events. ", t))
                                        .doOnNext(m -> logger.trace("event {} for aggregate {}", m, request.getAggregateId()))
                                        .contextWrite(c -> c.put(LAST_SEQ_KEY,
                                                                 new AtomicLong(request.getInitialSequence() - 1)))
                                        .name("event_stream")
                                        .tag("context", context)
                                        .tag("stream", "aggregate_events")
                                        .tag("origin", "client_request")
                                        .metrics();
                         })
                .onErrorResume(Flux::error);

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
        Long lastEventToken = eventStoreLocator.getEventStore(context)
                                               .lastEventToken(context)
                                               .block();
        return lastEventToken != null ? lastEventToken : -1;
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


    public void getFirstToken(String context, StreamObserver<TrackingToken> responseObserver) {
        checkConnection(context, responseObserver)
                .ifPresent(client -> client.firstEventToken(context)
                                           .map(token -> TrackingToken.newBuilder().setToken(token).build())
                                           .subscribe(responseObserver::onNext,
                                                      responseObserver::onError,
                                                      responseObserver::onCompleted));
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

    public void getLastToken(String context, StreamObserver<TrackingToken> responseObserver) {
        checkConnection(context, responseObserver)
                .ifPresent(client -> client.lastEventToken(context)
                                           .map(token -> TrackingToken.newBuilder().setToken(token).build())
                                           .subscribe(responseObserver::onNext,
                                                      responseObserver::onError,
                                                      responseObserver::onCompleted)
                );
    }

    public void getTokenAt(String context, long instant, StreamObserver<TrackingToken> responseObserver) {
        checkConnection(context, responseObserver)
                .ifPresent(client -> client.eventTokenAt(context,
                                                         Instant.ofEpochMilli(instant))
                                           .map(token -> TrackingToken.newBuilder().setToken(token).build())
                                           .subscribe(responseObserver::onNext,
                                                      responseObserver::onError,
                                                      responseObserver::onCompleted));
    }

    public void readHighestSequenceNr(String context, String aggregateId,
                                      StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
        checkConnection(context, responseObserver)
                .ifPresent(client -> client
                        .highestSequenceNumber(context, aggregateId)
                        .map(l -> ReadHighestSequenceNrResponse.newBuilder().setToSequenceNr(l).build())
                        .subscribe(responseObserver::onNext,
                                   responseObserver::onError,
                                   responseObserver::onCompleted)
                );
    }

    public StreamObserver<QueryEventsRequest> queryEvents(String context,
                                                          Authentication authentication,
                                                          StreamObserver<QueryEventsResponse> responseObserver) {
        return new StreamObserver<QueryEventsRequest>() {

            private final AtomicReference<Sinks.Many<QueryEventsRequest>> requestSinkRef = new AtomicReference<>();

            @Override
            public void onNext(QueryEventsRequest request) {
                if (requestSinkRef.compareAndSet(null, Sinks.many()
                                                            .unicast()
                                                            .onBackpressureBuffer())) {
                    EventStore eventStore = eventStoreLocator.getEventStore(context,
                                                                            request.getForceReadFromLeader());
                    if (eventStore == null) {
                        responseObserver.onError(new MessagingPlatformException(NO_EVENTSTORE,
                                                                                NO_EVENT_STORE_CONFIGURED + context));
                        return;
                    }
                    eventStore.queryEvents(context, requestSinkRef.get().asFlux(), authentication)
                              .subscribe(responseObserver::onNext,
                                         responseObserver::onError,
                                         responseObserver::onCompleted);
                }
                Sinks.EmitResult emitResult = requestSinkRef.get().tryEmitNext(request);
                if (emitResult.isFailure()) {
                    String message = String.format("%s: Error forwarding request to event store.", context);
                    logger.warn(message);
                    requestSinkRef.get().tryEmitError(new RuntimeException(message));
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
                Sinks.Many<QueryEventsRequest> sink = requestSinkRef.get();
                if (sink != null) {
                    sink.tryEmitComplete();
                }
            }
        };
    }

    public void listAggregateSnapshots(String context, Authentication authentication,
                                       GetAggregateSnapshotsRequest request,
                                       StreamObserver<SerializedEvent> responseObserver) {
        checkConnection(context, responseObserver).ifPresent(eventStore -> {
            try {
                eventStore.aggregateSnapshots(context, authentication, request)
                          .doOnCancel(() -> responseObserver.onError(GrpcExceptionBuilder.build(new RuntimeException(
                                  "Listing aggregate snapshots cancelled."))))
                          .subscribe(responseObserver::onNext,
                                     responseObserver::onError,
                                     responseObserver::onCompleted);
            } catch (RuntimeException t) {
                logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listAggregateSnapshots", t.getMessage(), t);
                responseObserver.onError(GrpcExceptionBuilder.build(t));
            }
        });
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
        private final AtomicReference<Sinks.Many<GetEventsRequest>> requestSink = new AtomicReference<>();
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
                requestSink.get().tryEmitNext(getEventsRequest);
            } catch (Exception reason) {
                logger.warn("Error on connection sending event to client: {}", reason.getMessage());
                requestSink.get().tryEmitComplete();
                removeTrackerInfo();
            }
        }

        private boolean registerEventTracker(GetEventsRequest getEventsRequest) {
            if (requestSink.compareAndSet(null, Sinks.many()
                                                     .unicast()
                                                     .onBackpressureBuffer())) {
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

                    eventStore.events(context, principal, requestSink.get().asFlux())
                              .doOnCancel(() -> StreamObserverUtils.error(responseObserver,
                                                                          GrpcExceptionBuilder.build(
                                                                                  new RuntimeException(
                                                                                          "Listing events canceled."))))
                              .subscribe(eventWithToken -> {
                                  responseObserver.onNext(eventWithToken.asInputStream());
                                  trackerInfo.incrementLastToken();
                              }, throwable -> {
                                  if (throwable instanceof IllegalStateException) {
                                      logger.debug(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listEvents",
                                                   throwable.getMessage());
                                  } else {
                                      logger.warn(ERROR_ON_CONNECTION_FROM_EVENT_STORE, "listEvents",
                                                  throwable.getMessage());
                                  }
                                  StreamObserverUtils.error(responseObserver, GrpcExceptionBuilder.build(throwable));
                                  removeTrackerInfo();
                              }, () -> {
                                  logger.info("{}: Tracking event processor closed", trackerInfo.context);
                                  removeTrackerInfo();
                                  StreamObserverUtils.complete(responseObserver);
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
            requestSink.get().tryEmitComplete();
            removeTrackerInfo();
        }
    }
}
