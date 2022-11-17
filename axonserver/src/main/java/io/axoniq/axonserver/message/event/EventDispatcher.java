/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.exception.ErrorCode.LIST_AGGREGATE_EVENTS_TIMEOUT;

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
    private final long listEventsTimeoutMillis;

    public EventDispatcher(EventStoreLocator eventStoreLocator,
                           MeterFactory meterFactory,
                           @Value("${axoniq.axonserver.event.aggregate.retry.attempts:3}") int maxRetryAttempts,
                           @Value("${axoniq.axonserver.event.aggregate.retry.delay:100}") long retryDelayMillis,
                           @Value("${axoniq.axonserver.event.aggregate.prefetch:5}") int aggregateEventsPrefetch,
                           @Value("${axoniq.axonserver.event.aggregate.timeout:30000}") long listEventsTimeoutMillis) {
        this.eventStoreLocator = eventStoreLocator;
        this.meterFactory = meterFactory;
        retrySpec = Retry.backoff(maxRetryAttempts, Duration.ofMillis(retryDelayMillis));
        this.aggregateEventsPrefetch = aggregateEventsPrefetch;
        this.listEventsTimeoutMillis = listEventsTimeoutMillis;
    }


    public Mono<Void> appendEvent(String context,
                                  Authentication authentication,
                                  Flux<SerializedEvent> eventFlux) {
        if (auditLog.isDebugEnabled()) {
            auditLog.debug("[{}@{}] Request to append events.", AuditLog.username(authentication), context);
        }

        return eventStoreLocator.eventStore(context).flatMap(eventStore -> {
            Flux<SerializedEvent> countingFlux =
                    eventFlux.doOnNext(event -> eventsCounter(context,
                                                              eventsCounter,
                                                              BaseMetricName.AXON_EVENTS).mark());
            return eventStore.appendEvents(context, countingFlux, authentication);
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
        if (auditLog.isDebugEnabled()) {
            auditLog.debug("[{}@{}] Request to list events for {}.",
                           AuditLog.username(authentication),
                           context,
                           request.getAggregateId());
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
                                               .timeout(Duration.ofMillis(listEventsTimeoutMillis))
                                               .onErrorMap(TimeoutException.class,
                                                           e -> new MessagingPlatformException(
                                                                   LIST_AGGREGATE_EVENTS_TIMEOUT,
                                                                   "Timeout exception: No events were emitted from event store in last "
                                                                           + listEventsTimeoutMillis
                                                                           + "ms. Check the logs for virtual machine errors like OutOfMemoryError."))
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
                                               .doOnError(t -> logger.error("Error during reading aggregate events. ",
                                                                            t))
                                               .doOnNext(m -> logger.trace("event {} for aggregate {}",
                                                                           m,
                                                                           request.getAggregateId()))
                                               .contextWrite(c -> c.put(LAST_SEQ_KEY,
                                                                        new AtomicLong(
                                                                                request.getInitialSequence() - 1)))
                                               .name("event_stream")
                                               .tag("context", context)
                                               .tag("stream", "aggregate_events")
                                               .tag("origin", "client_request")
                                               .metrics();
                                })
                                .onErrorResume(Flux::error);
    }

    public Flux<SerializedEventWithToken> events(String context, Authentication principal,
                                                 Flux<GetEventsRequest> requestFlux) {
        return requestFlux.switchOnFirst((signal, rf) -> {
            if (signal.isOnNext()) {
                GetEventsRequest request = signal.get();
                EventTrackerInfo trackerInfo = new EventTrackerInfo(request.getClientId(),
                                                                    context,
                                                                    request.getTrackingToken() - 1);
                ClientStreamIdentification clientStreamIdentification =
                        new ClientStreamIdentification(trackerInfo.context, trackerInfo.client);
                trackingEventProcessors.computeIfAbsent(clientStreamIdentification, key -> new CopyOnWriteArrayList<>())
                                       .add(trackerInfo);
                logger.info("Starting tracking event processor for {}:{} - {}",
                            request.getClientId(),
                            request.getComponentName(),
                            request.getTrackingToken());
                return eventStoreLocator.eventStore(context, request.getForceReadFromLeader())
                                        .flatMapMany(eventStore -> eventStore.events(context,
                                                                                     principal,
                                                                                     rf))
                                        .doOnNext(serializedEventWithToken -> trackerInfo.incrementLastToken())
                                        .doFinally(s -> removeTrackerInfo(trackerInfo));
            } else if (signal.isOnError()) {
                return Flux.error(signal.getThrowable());
            } else {
                return Flux.empty();
            }
        });
    }

    private void removeTrackerInfo(@Nonnull EventTrackerInfo trackerInfo) {
        logger.info("Removed tracker info {}", trackerInfo);
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

    public long getNrOfEvents(String context) {
        Long lastEventToken = eventStoreLocator.eventStore(context)
                                               .flatMap(eventStore -> eventStore.lastEventToken(context))
                                               .switchIfEmpty(Mono.just(-1L))
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

    public Mono<Long> firstEventToken(String context) {
        return eventStoreLocator.eventStore(context, false)
                                .flatMap(eventStore -> eventStore.firstEventToken(context));
    }

    public Mono<Long> lastEventToken(String context) {
        return eventStoreLocator.eventStore(context)
                                .flatMap(eventStore -> eventStore.lastEventToken(context));
    }

    public Mono<Long> eventTokenAt(String context, Instant timestamp) {
        return eventStoreLocator.eventStore(context, false)
                                .flatMap(eventStore -> eventStore.eventTokenAt(context, timestamp));
    }

    public Mono<Long> highestSequenceNumber(String context, String aggregateId) {
        return eventStoreLocator.eventStore(context)
                                .flatMap(eventStore -> eventStore.highestSequenceNumber(context, aggregateId));
    }

    public Flux<QueryEventsResponse> queryEvents(String context, Authentication authentication,
                                                 Flux<QueryEventsRequest> requestFlux) {
        return requestFlux.switchOnFirst((signal, rf) -> {
            if (signal.isOnNext()) {
                QueryEventsRequest request = signal.get();
                return eventStoreLocator.eventStore(context, request.getForceReadFromLeader())
                                        .flatMapMany(es -> es.queryEvents(context,
                                                                          rf,
                                                                          authentication));
            } else if (signal.isOnError()) {
                return Flux.error(signal.getThrowable());
            } else {
                return Flux.empty();
            }
        });
    }

    public Flux<SerializedEvent> aggregateSnapshots(String context, Authentication authentication,
                                                    GetAggregateSnapshotsRequest request) {
        return eventStoreLocator.eventStore(context)
                                .flatMapMany(eventStore -> eventStore.aggregateSnapshots(context,
                                                                                         authentication,
                                                                                         request));
    }

    public MeterFactory.RateMeter eventRate(String context) {
        return eventsCounter(context, eventsCounter, BaseMetricName.AXON_EVENTS);
    }

    public MeterFactory.RateMeter snapshotRate(String context) {
        return eventsCounter(context, snapshotCounter, BaseMetricName.AXON_SNAPSHOTS);
    }

    private static class EventTrackerInfo {

        private final String client;
        private final String context;
        private final AtomicLong lastToken;

        public EventTrackerInfo(String client, String context, long lastToken) {
            this.client = client;
            this.context = context;
            this.lastToken = new AtomicLong(lastToken);
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
                    "client='" + client + '\'' +
                    ", context='" + context + '\'' +
                    ", lastToken=" + lastToken +
                    '}';
        }
    }
}
