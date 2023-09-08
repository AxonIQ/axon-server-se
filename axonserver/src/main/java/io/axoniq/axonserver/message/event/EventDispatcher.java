/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.exception.ErrorCode;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.exception.ErrorCode.LIST_AGGREGATE_EVENTS_TIMEOUT;
import static io.axoniq.axonserver.metric.MeterFactory.CONTEXT;
import static io.axoniq.axonserver.metric.MeterFactory.ERROR_CODE;

/**
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @author Stefan Dragisic
 */
@Component
public class EventDispatcher {

    private static final Logger auditLog = AuditLog.getLogger();

    static final String ERROR_ON_CONNECTION_FROM_EVENT_STORE = "{}:  Error on connection from event store: {}";
    private final Logger logger = LoggerFactory.getLogger(EventDispatcher.class);
    private final EventStoreLocator eventStoreLocator;
    private final MeterFactory meterFactory;
    private final boolean legacyMetricsEnabled;
    private final Map<ClientStreamIdentification, List<EventTrackerInfo>> trackingEventProcessors = new ConcurrentHashMap<>();
    private final Map<String, MeterFactory.RateMeter> eventsCounter = new ConcurrentHashMap<>();
    private final Map<String, MeterFactory.RateMeter> snapshotCounter = new ConcurrentHashMap<>();
    private final Map<String, MeterFactory.RateMeter> readEventsCounter = new ConcurrentHashMap<>();
    private final Map<String, MeterFactory.RateMeter> readSnapshotsCounter = new ConcurrentHashMap<>();
    @Value("${axoniq.axonserver.read-sequence-validation-strategy:LOG}")
    private SequenceValidationStrategy sequenceValidationStrategy = SequenceValidationStrategy.LOG;
    private final RetryBackoffSpec retrySpec;
    private final int aggregateEventsPrefetch;
    private final long listEventsTimeoutMillis;
    private final Map<String, AtomicInteger> activeAppendEventsPerContext = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeAppendSnapshotPerContext = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeReadAggregateEventsPerContext = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeReadSnapshotsPerContext = new ConcurrentHashMap<>();

    public EventDispatcher(EventStoreLocator eventStoreLocator,
                           MeterFactory meterFactory,
                           @Value("${axoniq.axonserver.event.aggregate.retry.attempts:3}") int maxRetryAttempts,
                           @Value("${axoniq.axonserver.event.aggregate.retry.delay:100}") long retryDelayMillis,
                           @Value("${axoniq.axonserver.event.aggregate.prefetch:5}") int aggregateEventsPrefetch,
                           @Value("${axoniq.axonserver.event.aggregate.timeout:30000}") long listEventsTimeoutMillis,
                           @Value("${axoniq.axonserver.legacy-metrics-enabled:true}") boolean legacyMetricsEnabled) {
        this.eventStoreLocator = eventStoreLocator;
        this.meterFactory = meterFactory;
        this.legacyMetricsEnabled = legacyMetricsEnabled;
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

        long start = System.currentTimeMillis();
        return eventStoreLocator.eventStore(context).flatMap(eventStore -> {
            Flux<SerializedEvent> countingFlux =
                    eventFlux.doOnNext(event -> eventsCounter(context,
                                                              eventsCounter,
                                                              BaseMetricName.APPEND_EVENT_THROUGHPUT,
                                                              BaseMetricName.AXON_EVENTS).mark());
            return eventStore.appendEvents(context, countingFlux, authentication)
                             .doFirst(() -> {
                                 AtomicInteger counter = activeAppendEventsPerContext.computeIfAbsent(context,
                                                                                                      c -> new AtomicInteger());
                                 counter.incrementAndGet();
                                 meterFactory.gauge(BaseMetricName.APPEND_EVENT_ACTIVE,
                                                    Tags.of(CONTEXT, context),
                                                    counter::get);
                             })
                             .doOnSuccess(ignored -> meterFactory.timer(BaseMetricName.APPEND_EVENT_DURATION,
                                                                        Tags.of(CONTEXT,
                                                                                context
                                                                        )).record(System.currentTimeMillis() - start,
                                                                                  TimeUnit.MILLISECONDS))
                             .doOnError(t -> meterFactory.counter(BaseMetricName.APPEND_EVENT_ERRORS,
                                                                  Tags.of(CONTEXT,
                                                                          context,
                                                                          ERROR_CODE,
                                                                          ErrorCode.fromException(t).getCode()))
                                                         .increment())
                             .doOnTerminate(() -> activeAppendEventsPerContext.get(context).decrementAndGet());
        });
    }

    private MeterFactory.RateMeter eventsCounter(String context, Map<String, MeterFactory.RateMeter> eventsCounter,
                                                 BaseMetricName eventsMetricName, BaseMetricName legacyMetricName) {
        return eventsCounter.computeIfAbsent(context, c -> meterFactory.rateMeter(eventsMetricName,
                                                                                  legacyMetricsEnabled ? legacyMetricName : null,
                                                                                  Tags.of(CONTEXT,
                                                                                          context)));
    }

    public Mono<Void> appendSnapshot(String context, Event snapshot, Authentication authentication) {
        if (auditLog.isDebugEnabled()) {
            auditLog.debug("[{}@{}] Request to list events for {}.",
                           AuditLog.username(authentication),
                           context,
                           snapshot.getAggregateIdentifier());
        }
        long start = System.currentTimeMillis();
        return eventStoreLocator.eventStore(context)
                                .flatMap(eventStore -> eventStore.appendSnapshot(context, snapshot, authentication)
                                                                 .doFirst(() -> {
                                                                     AtomicInteger counter = activeAppendSnapshotPerContext.computeIfAbsent(
                                                                             context,
                                                                             c -> new AtomicInteger());
                                                                     counter.incrementAndGet();
                                                                     meterFactory.gauge(BaseMetricName.APPEND_SNAPSHOT_ACTIVE,
                                                                                        Tags.of(CONTEXT, context),
                                                                                        counter::get);
                                                                 })
                                                                 .doOnSuccess(v -> {
                                                                     eventsCounter(context,
                                                                                   snapshotCounter,
                                                                                   BaseMetricName.APPEND_SNAPSHOT_THROUGHPUT,
                                                                                   BaseMetricName.AXON_SNAPSHOTS).mark();
                                                                     meterFactory.timer(BaseMetricName.APPEND_SNAPSHOT_DURATION,
                                                                                        Tags.of(CONTEXT,
                                                                                                context
                                                                                        )).record(
                                                                             System.currentTimeMillis() - start,
                                                                             TimeUnit.MILLISECONDS);
                                                                 })
                                                                 .doOnError(t -> {
                                                                     logger.warn(
                                                                             ERROR_ON_CONNECTION_FROM_EVENT_STORE,
                                                                             "appendSnapshot",
                                                                             t.getMessage());
                                                                     meterFactory.counter(BaseMetricName.APPEND_SNAPSHOT_ERRORS,
                                                                                          Tags.of(CONTEXT,
                                                                                                  context,
                                                                                                  ERROR_CODE,
                                                                                                  ErrorCode.fromException(
                                                                                                          t).getCode()))
                                                                                 .increment();
                                                                 })
                                                                 .doOnTerminate(() -> activeAppendSnapshotPerContext.get(
                                                                         context).decrementAndGet()));
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

        long start = System.currentTimeMillis();
        return eventStoreLocator.eventStore(context)
                                .flatMapMany(eventStore -> {
                                    final String LAST_SEQ_KEY = "__LAST_SEQ";
                                    Tags tags = Tags.of(CONTEXT, context);
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
                                               .doFirst(() -> {
                                                   AtomicInteger counter = activeReadAggregateEventsPerContext.computeIfAbsent(
                                                           context,
                                                           c -> new AtomicInteger());
                                                   counter.incrementAndGet();
                                                   meterFactory.gauge(BaseMetricName.READ_AGGREGATE_EVENTS_ACTIVE,
                                                                      tags,
                                                                      counter::get);
                                                   readEventsCounter.computeIfAbsent(context,
                                                                                     c -> meterFactory.rateMeter(
                                                                                             BaseMetricName.READ_AGGREGATE_EVENTS_THROUGHPUT,
                                                                                             tags)).mark();
                                               })
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
                                               .doOnError(t -> {
                                                   logger.error("Error during reading aggregate events. ",
                                                                t);
                                                   meterFactory.counter(BaseMetricName.READ_AGGREGATE_EVENTS_ERRORS,
                                                                        Tags.of(CONTEXT,
                                                                                context,
                                                                                ERROR_CODE,
                                                                                ErrorCode.fromException(t).getCode()))
                                                               .increment();
                                               })
                                               .doOnNext(m -> logger.trace("event {} for aggregate {}",
                                                                           m,
                                                                           request.getAggregateId()))
                                               .doOnComplete(() -> meterFactory.timer(BaseMetricName.READ_AGGREGATE_EVENTS_DURATION,
                                                                                      tags)
                                                                               .record(System.currentTimeMillis()
                                                                                               - start,
                                                                                       TimeUnit.MILLISECONDS))
                                               .doOnTerminate(() -> activeReadAggregateEventsPerContext.get(context)
                                                                                                       .decrementAndGet())
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
        long start = System.currentTimeMillis();
        Tags tags = Tags.of(CONTEXT, context);
        return eventStoreLocator.eventStore(context)
                                .flatMapMany(eventStore -> eventStore.aggregateSnapshots(context,
                                                                                         authentication,
                                                                                         request))
                                .doFirst(() -> {
                                    AtomicInteger counter = activeReadSnapshotsPerContext.computeIfAbsent(context,
                                                                                                          c -> new AtomicInteger());
                                    counter.incrementAndGet();
                                    meterFactory.gauge(BaseMetricName.READ_SNAPSHOT_ACTIVE,
                                                       tags,
                                                       counter::get);
                                    readSnapshotsCounter.computeIfAbsent(context,
                                                                         c -> meterFactory.rateMeter(BaseMetricName.READ_SNAPSHOT_THROUGHPUT,
                                                                                                     tags)).mark();
                                })
                                .doOnError(t -> meterFactory.counter(BaseMetricName.READ_AGGREGATE_EVENTS_ERRORS,
                                                                     Tags.of(CONTEXT,
                                                                             context,
                                                                             ERROR_CODE,
                                                                             ErrorCode.fromException(t).getCode()))
                                                            .increment())
                                .doOnComplete(() -> meterFactory.timer(BaseMetricName.READ_SNAPSHOT_DURATION,
                                                                       tags)
                                                                .record(System.currentTimeMillis() - start,
                                                                        TimeUnit.MILLISECONDS))
                                .doOnTerminate(() -> activeReadSnapshotsPerContext.get(context).decrementAndGet());
    }

    public MeterFactory.RateMeter eventRate(String context) {
        return eventsCounter(context,
                             eventsCounter,
                             BaseMetricName.APPEND_EVENT_THROUGHPUT,
                             BaseMetricName.AXON_EVENTS);
    }

    public MeterFactory.RateMeter snapshotRate(String context) {
        return eventsCounter(context,
                             snapshotCounter,
                             BaseMetricName.APPEND_SNAPSHOT_THROUGHPUT,
                             BaseMetricName.AXON_SNAPSHOTS);
    }

    public void deleteMetrics(String context) {
        MeterFactory.RateMeter meter = eventsCounter.remove(context);
        if (meter != null) {
            meter.remove();
        }
        meter = snapshotCounter.remove(context);
        if (meter != null) {
            meter.remove();
        }
        meter = readEventsCounter.remove(context);
        if (meter != null) {
            meter.remove();
        }
        meter = readSnapshotsCounter.remove(context);
        if (meter != null) {
            meter.remove();
        }
        meterFactory.remove(BaseMetricName.APPEND_EVENT_DURATION, CONTEXT, context);
        meterFactory.remove(BaseMetricName.APPEND_EVENT_ERRORS, CONTEXT, context);
        meterFactory.remove(BaseMetricName.APPEND_SNAPSHOT_DURATION, CONTEXT, context);
        meterFactory.remove(BaseMetricName.APPEND_SNAPSHOT_ERRORS, CONTEXT, context);
        meterFactory.remove(BaseMetricName.READ_AGGREGATE_EVENTS_ERRORS, CONTEXT, context);
        meterFactory.remove(BaseMetricName.READ_AGGREGATE_EVENTS_DURATION, CONTEXT, context);
        meterFactory.remove(BaseMetricName.READ_SNAPSHOT_DURATION, CONTEXT, context);
        meterFactory.remove(BaseMetricName.READ_SNAPSHOT_ERRORS, CONTEXT, context);
        meterFactory.remove(BaseMetricName.APPEND_EVENT_ACTIVE, CONTEXT, context);
        meterFactory.remove(BaseMetricName.APPEND_SNAPSHOT_ACTIVE, CONTEXT, context);
        meterFactory.remove(BaseMetricName.READ_AGGREGATE_EVENTS_ACTIVE, CONTEXT, context);
        meterFactory.remove(BaseMetricName.READ_SNAPSHOT_ACTIVE, CONTEXT, context);

        activeReadSnapshotsPerContext.remove(context);
        activeReadAggregateEventsPerContext.remove(context);
        activeAppendSnapshotPerContext.remove(context);
        activeAppendEventsPerContext.remove(context);
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
