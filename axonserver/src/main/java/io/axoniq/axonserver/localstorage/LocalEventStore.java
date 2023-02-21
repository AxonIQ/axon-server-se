/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.GetTokenAtRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.interceptor.DefaultExecutionContext;
import io.axoniq.axonserver.interceptor.EventInterceptors;
import io.axoniq.axonserver.localstorage.query.QueryEventsRequestStreamObserver;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventStoreTransformer;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.plugin.RequestRejectedException;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.util.CloseableIterator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Component that handles the actual interaction with the event store.
 *
 * @author Marc Gathier
 * @author Stefan Dragisic
 *
 * @since 4.0
 */
@Component
public class LocalEventStore implements io.axoniq.axonserver.message.event.EventStore,
        EventStoreTransformer,
        SmartLifecycle {

    private static final Confirmation CONFIRMATION = Confirmation.newBuilder().setSuccess(true).build();
    private final Logger logger = LoggerFactory.getLogger(LocalEventStore.class);
    private final Map<String, Workers> workersMap = new ConcurrentHashMap<>();
    private final EventStoreFactory eventStoreFactory;
    private final ExecutorService dataFetcher;
    private final ExecutorService dataWriter;
    private final MeterFactory meterFactory;
    private final StorageTransactionManagerFactory storageTransactionManagerFactory;
    private final EventInterceptors eventInterceptors;

    /**
     * Maximum number of blacklisted events to be skipped before it will send a blacklisted event anyway. If almost all
     * events would be ignored due to blacklist, tracking tokens on client applications would never be updated.
     */
    private final int blacklistedSendAfter;
    private final EventDecorator eventDecorator;
    private volatile boolean running;
    @Value("${axoniq.axonserver.query.limit:200}")
    private long defaultLimit = 200;
    @Value("${axoniq.axonserver.query.timeout:300000}")
    private long timeout = 300000;
    @Value("${axoniq.axonserver.new-permits-timeout:120000}")
    private long newPermitsTimeout = 120000;
    @SuppressWarnings("FieldMayBeFinal") @Value("${axoniq.axonserver.check-sequence-nr-for-snapshots:true}")
    private boolean checkSequenceNrForSnapshots = true;

    public LocalEventStore(EventStoreFactory eventStoreFactory,
                           MeterRegistry meterFactory,
                           StorageTransactionManagerFactory storageTransactionManagerFactory,
                           EventInterceptors eventInterceptors
    ) {
        this(eventStoreFactory,
             new MeterFactory(meterFactory, new DefaultMetricCollector()),
             storageTransactionManagerFactory,
             eventInterceptors,
             new DefaultEventDecorator(),
             1000,
             24,
             8);
    }

    @Autowired
    public LocalEventStore(EventStoreFactory eventStoreFactory,
                           MeterFactory meterFactory,
                           StorageTransactionManagerFactory storageTransactionManagerFactory,
                           EventInterceptors eventInterceptors,
                           EventDecorator eventDecorator,
                           @Value("${axoniq.axonserver.blacklisted-send-after:1000}") int blacklistedSendAfter,
                           @Value("${axoniq.axonserver.data-fetcher-threads:24}") int fetcherThreads,
                           @Value("${axoniq.axonserver.data-writer-threads:8}") int writerThreads) {
        this.eventStoreFactory = eventStoreFactory;
        this.meterFactory = meterFactory;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
        this.eventInterceptors = eventInterceptors;
        this.blacklistedSendAfter = blacklistedSendAfter;
        this.dataFetcher = Executors.newFixedThreadPool(fetcherThreads, new CustomizableThreadFactory("data-fetcher-"));
        DataFetcherSchedulerProvider.setDataFetcher(dataFetcher);
        this.dataWriter = Executors.newFixedThreadPool(writerThreads, new CustomizableThreadFactory("data-writer-"));
        this.eventDecorator = eventDecorator;
    }

    public void initContext(String context, boolean validating) {
        initContext(context, validating, 0, 0);
    }

    public void initContext(String context, boolean validating, long defaultFirstEventIndex,
                            long defaultFirstSnapshotIndex) {
        try {
            Workers workers = workersMap.computeIfAbsent(context, Workers::new);
            workers.ensureInitialized(validating, defaultFirstEventIndex, defaultFirstSnapshotIndex);
        } catch (RuntimeException ex) {
            workersMap.remove(context);
            throw ex;
        }
    }

    /**
     * Deletes the specified context including all data.
     *
     * @param context the name of the context
     */
    public void deleteContext(String context) {
        deleteContext(context, false);
    }

    /**
     * Deletes the specified context, optionally keeping the data
     *
     * @param context  the name of the context
     * @param keepData flag to set if the data must be preserved
     */
    public void deleteContext(String context, boolean keepData) {
        Workers workers = workersMap.remove(context);
        if (workers == null) {
            return;
        }
        workers.close(!keepData);
    }

    /**
     * Deletes all event data from the context. Context remains alive.
     *
     * @param context the context to be cleared
     */
    @Override
    public Mono<Void> deleteAllEventData(String context) {
        return Mono.create(sink -> {
            try {
                Workers workers = workersMap.remove(context);
                if (workers != null) {
                    workers.close(true);
                    initContext(context, false);
                }
                sink.success();
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public void cancel(String context) {
        Workers workers = workersMap.get(context);
        if (workers == null) {
            return;
        }

        workers.eventWriteStorage.cancelPendingTransactions();
        workers.cancelTrackingEventProcessors();
    }

    private Workers workers(String context) {
        Workers workers = workersMap.get(context);
        if (workers == null) {
            throw new MessagingPlatformException(ErrorCode.NO_EVENTSTORE, "Missing worker for context: " + context);
        }
        return workers;
    }

    @Override
    public Flux<Long> transformEvents(String context,
                                      int segmentVersion,
                                      Flux<EventWithToken> transformedEvents) {
        return workersMap.get(context)
                .eventStorageEngine
                .transformContents(segmentVersion, transformedEvents)
                .subscribeOn(Schedulers.fromExecutorService(dataFetcher));
    }

    /*----------------------HANDLE APPEND SNAPSHOT----------------------*/

    @Override
    public Mono<Void> appendSnapshot(String context, Event snapshot, Authentication authentication) {
        DefaultExecutionContext executionContext = new DefaultExecutionContext(context, authentication);
        return Mono.just(snapshot)
                .publishOn(Schedulers.fromExecutorService(dataWriter))
                .doFirst(() -> validateSnapshotSequence(context, snapshot))
                .map(s -> eventInterceptors.interceptSnapshot(snapshot, executionContext))
                .flatMap(interceptedSnapshot -> storeSnapshot(context, interceptedSnapshot))
                .doOnSuccess(storedSnapshot -> eventInterceptors.interceptSnapshotPostCommit(storedSnapshot, executionContext))
                .transform(pipeline -> handleStoreSnapshotErrors(context, executionContext, pipeline))
                .transform(pipeline -> attachStoreSnapshotMetrics(context, pipeline))
                .then();
    }

    private void validateSnapshotSequence(String context, Event snapshot) {
        if (checkSequenceNrForSnapshots) {
            long seqNr = workers(context).aggregateReader.readHighestSequenceNr(snapshot.getAggregateIdentifier());
            if (seqNr < snapshot.getAggregateSequenceNumber()) {
                throw new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE,
                        String.format(
                                "Invalid sequence number while storing snapshot. Highest aggregate %s sequence number: %d, snapshot sequence %d.",
                                snapshot.getAggregateIdentifier(),
                                seqNr,
                                snapshot.getAggregateSequenceNumber()));
            }
        }
    }

    @Nonnull
    private Mono<Event> storeSnapshot(String context, Event snapshot) {
        return workers(context).snapshotWriteStorage
                .store(snapshot)
                .thenReturn(snapshot);
    }

    private Mono<Event> handleStoreSnapshotErrors(String context, DefaultExecutionContext executionContext, Mono<Event> pipeline) {
        return pipeline
                .doOnError(t -> logger.error("{}: Append snapshot failed", context, t))
                .doOnError(executionContext::compensate)
                .onErrorMap(RequestRejectedException.class, t -> new MessagingPlatformException(ErrorCode.SNAPSHOT_REJECTED_BY_INTERCEPTOR,
                        "Snapshot rejected by interceptor",
                        t))
                .onErrorMap(t -> !t.getClass().isAssignableFrom(MessagingPlatformException.class), MessagingPlatformException::create);
    }

    private Mono<Event> attachStoreSnapshotMetrics(String context, Mono<Event> pipeline) {
        return pipeline
                .name("append_stream")
                .tag("context", context)
                .tag("stream", "snapshots")
                .tag("origin", "local_event_store")
                .metrics();
    }

    /*----------------------HANDLE APPEND EVENT----------------------*/

    @Override
    public Mono<Void> appendEvents(String context, Flux<SerializedEvent> events, Authentication authentication) {
        DefaultExecutionContext executionContext = new DefaultExecutionContext(context, authentication);
        return events
                .publishOn(Schedulers.fromExecutorService(dataWriter))
                .map(SerializedEvent::asEvent)
                .map(e -> eventInterceptors
                        .interceptEvent(e, executionContext))
                .transform(serializedEvents -> prepareAndStoreBatch(context, executionContext, serializedEvents))
                .transform(pipeline -> handleStoreEventErrors(executionContext, pipeline))
                .transform(pipeline -> attachStoreEventMetrics(context, pipeline))
                .then();
    }

    @Nonnull
    private Flux<Void> prepareAndStoreBatch(String context, DefaultExecutionContext executionContext, Flux<Event> serializedEvents) {
        return serializedEvents
                .buffer()
                .transform(batches -> storeBatch(context, executionContext, batches));
    }

    @Nonnull
    private Flux<Void> storeBatch(String context, DefaultExecutionContext executionContext, Flux<List<Event>> eventBatches) {
        return eventBatches.doOnNext(interceptedEvents -> eventInterceptors.interceptEventsPreCommit(interceptedEvents, executionContext))
                .concatMap(interceptedEvents -> workers(context)
                        .eventWriteStorage
                        .storeBatch(interceptedEvents)
                        .doOnSuccess(unused -> eventInterceptors.interceptEventsPostCommit(interceptedEvents,
                                executionContext))
                );
    }

    @Nonnull
    private Flux<Void> handleStoreEventErrors(DefaultExecutionContext executionContext, Flux<Void> pipeline) {
        return pipeline
                .doOnError(t -> logger.error("Error occurred during store batch operation: ", t))
                .doOnError(executionContext::compensate)
                .onErrorMap(RequestRejectedException.class, t -> new MessagingPlatformException(ErrorCode.EVENT_REJECTED_BY_INTERCEPTOR,
                        "Event rejected by interceptor",
                        t))
                .onErrorMap(t -> !t.getClass().isAssignableFrom(MessagingPlatformException.class), MessagingPlatformException::create);
    }

    private Flux<Void> attachStoreEventMetrics(String context, Flux<Void> pipeline) {
        return pipeline
                .name("append_stream")
                .tag("context", context)
                .tag("stream", "events")
                .tag("origin", "local_event_store")
                .metrics();
    }

    /*---------------------- LIST EVENT----------------------*/

    @Override
    public Flux<SerializedEvent> aggregateEvents(String context,
                                                 Authentication authentication,
                                                 GetAggregateEventsRequest request) {
        EventDecorator activeEventDecorator = eventInterceptors.noReadInterceptors(context) ?
                eventDecorator :
                new InterceptorAwareEventDecorator(context, authentication);
        AggregateReader aggregateReader = workers(context).aggregateReader;
        return aggregateReader
                .events(request.getAggregateId(),
                        request.getAllowSnapshots(),
                        request.getInitialSequence(),
                        getMaxSequence(request),
                        request.getMinToken())
                .map(activeEventDecorator::decorateEvent)
                .subscribeOn(Schedulers.fromExecutorService(dataFetcher))
                .transform(f -> count(f, counter -> {
                    if (counter == 0) {
                        logger.debug("Aggregate not found: {}", request);
                    }
                }))
                .name("event_stream")
                .tag("context", context)
                .tag("stream", "aggregate_events")
                .tag("origin", "local_event_store")
                .metrics();
    }

    private <T> Publisher<T> count(Flux<T> flux, IntConsumer doOnComplete) {
        final String contextKey = "__COUNTER";
        return flux.doOnEach(signal -> {
            if (signal.isOnNext()) {
                AtomicInteger counter = signal.getContextView().get(contextKey);
                counter.incrementAndGet();
            } else if (signal.isOnComplete()) {
                AtomicInteger counter = signal.getContextView().get(contextKey);
                doOnComplete.accept(counter.get());
            }
        }).contextWrite(ctx -> ctx.put(contextKey, new AtomicInteger()));
    }

    private void runInDataFetcherPool(Runnable task, Consumer<Exception> onError) {
        runInPool(dataFetcher, task, onError);
    }

    private void runInPool(ExecutorService threadPool, Runnable task, Consumer<Exception> onError) {
        try {
            threadPool.submit(() -> {
                // we will not start new reader tasks when a shutdown was initialized. Behavior is similar to submitting tasks after shutdown
                if (!running) {
                    onError.accept(new RejectedExecutionException("Cannot load events. AxonServer is shutting down"));
                } else {
                    try {
                        task.run();
                    } catch (Exception ex) {
                        onError.accept(ex);
                    }
                }
            });
        } catch (Exception e) {
            // in case we didn't manage to schedule the task to run.
            onError.accept(e);
        }
    }

    private long getMaxSequence(GetAggregateEventsRequest request) {
        if (request.getMaxSequence() > 0) {
            return request.getMaxSequence();
        }
        return Long.MAX_VALUE;
    }

    @Override
    public Flux<SerializedEvent> aggregateSnapshots(String context, Authentication authentication,
                                                    GetAggregateSnapshotsRequest request) {
        return Flux.create(
                sink -> listAggregateSnapshots(context, authentication, request, new StreamObserver<SerializedEvent>() {
                    @Override
                    public void onNext(SerializedEvent serializedEvent) {
                        sink.next(serializedEvent);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        sink.error(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        sink.complete();
                    }
                }));
    }

    private void listAggregateSnapshots(String context,
                                        Authentication authentication,
                                        GetAggregateSnapshotsRequest request,
                                        StreamObserver<SerializedEvent> responseStreamObserver) {
        runInDataFetcherPool(() -> {
            if (request.getMaxSequence() >= 0) {
                EventDecorator activeEventDecorator = eventInterceptors.noSnapshotReadInterceptors(context) ?
                        eventDecorator :
                        new InterceptorAwareEventDecorator(context, authentication);

                workers(context).aggregateReader.readSnapshots(request.getAggregateId(),
                                                               request.getInitialSequence(),
                                                               request.getMaxSequence(),
                                                               request.getMaxResults(),
                                                               snapshot -> responseStreamObserver
                                                                       .onNext(activeEventDecorator
                                                                                       .decorateEvent(snapshot)));
            }
            responseStreamObserver.onCompleted();
        }, error -> {
            logger.warn("Problem encountered while reading snapshot for aggregate " + request.getAggregateId(), error);
            responseStreamObserver.onError(error);
        });
    }

    @Override
    public Flux<SerializedEventWithToken> events(String context, Authentication authentication,
                                                 Flux<GetEventsRequest> requestFlux) {
        return Flux.create(sink -> {
            StreamObserver<GetEventsRequest> requestStreamObserver =
                    listEvents(context,
                               authentication,
                               new StreamObserver<SerializedEventWithToken>() {
                                   @Override
                                   public void onNext(SerializedEventWithToken event) {
                                       sink.next(event);
                                   }

                                   @Override
                                   public void onError(Throwable throwable) {
                                       sink.error(throwable);
                                   }

                                   @Override
                                   public void onCompleted() {
                                       sink.complete();
                                   }
                               });
            requestFlux.subscribe(requestStreamObserver::onNext,
                                  requestStreamObserver::onError,
                                  requestStreamObserver::onCompleted);
        });
    }

    private StreamObserver<GetEventsRequest> listEvents(String context, Authentication authentication,
                                                        StreamObserver<SerializedEventWithToken> responseStreamObserver) {
        return new StreamObserver<GetEventsRequest>() {
            private final AtomicReference<TrackingEventProcessorManager.EventTracker> controllerRef = new AtomicReference<>();

            @Override
            public void onNext(GetEventsRequest getEventsRequest) {
                TrackingEventProcessorManager.EventTracker controller = controllerRef.updateAndGet(c -> {
                    if (c == null) {
                        EventDecorator activeEventDecorator =
                                new InterceptorAwareEventDecorator(context, authentication);
                        return workers(context).createEventTracker(getEventsRequest.getTrackingToken(),
                                                                   getEventsRequest.getClientId(),
                                                                   getEventsRequest.getForceReadFromLeader(),
                                                                   new StreamObserver<SerializedEventWithToken>() {
                                                                       @Override
                                                                       public void onNext(
                                                                               SerializedEventWithToken eventWithToken) {
                                                                           responseStreamObserver.onNext(
                                                                                   activeEventDecorator
                                                                                           .decorateEventWithToken(
                                                                                                   eventWithToken));
                                                                       }

                                                                       @Override
                                                                       public void onError(Throwable throwable) {
                                                                           responseStreamObserver.onError(throwable);
                                                                       }

                                                                       @Override
                                                                       public void onCompleted() {
                                                                           responseStreamObserver.onCompleted();
                                                                       }
                                                                   });
                    }
                    return c;
                });

                controller.addPermits((int) getEventsRequest.getNumberOfPermits());
                if (getEventsRequest.getBlacklistCount() > 0) {
                    controller.addBlacklist(getEventsRequest.getBlacklistList());
                }
                controller.start();
            }

            @Override
            public void onError(Throwable throwable) {
                Optional.ofNullable(controllerRef.get())
                        .ifPresent(TrackingEventProcessorManager.EventTracker::close);
                StreamObserverUtils.complete(responseStreamObserver);
            }

            @Override
            public void onCompleted() {
                Optional.ofNullable(controllerRef.get())
                        .ifPresent(TrackingEventProcessorManager.EventTracker::close);
                StreamObserverUtils.complete(responseStreamObserver);
            }
        };
    }

    @Override
    public Mono<Long> firstEventToken(String context) {
        return Mono.just(workers(context).eventStreamReader.getFirstToken());
    }

    @Override
    public Mono<Long> lastEventToken(String context) {
        return Mono.just(workers(context).eventStorageEngine.getLastToken());
    }

    @Override
    public Mono<Long> eventTokenAt(String context, Instant timestamp) {
        return Mono.create(sink ->
                                   getTokenAt(context,
                                              GetTokenAtRequest.newBuilder().setInstant(timestamp.toEpochMilli())
                                                               .build(),
                                              new StreamObserver<TrackingToken>() {
                                                  @Override
                                                  public void onNext(TrackingToken trackingToken) {
                                                      sink.success(trackingToken.getToken());
                                                  }

                                                  @Override
                                                  public void onError(Throwable throwable) {
                                                      sink.error(throwable);
                                                  }

                                                  @Override
                                                  public void onCompleted() {
                                                      //nothing to do, already completed
                                                  }
                                              }));
    }

    private void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver) {
        runInDataFetcherPool(() -> {
            long token = workers(context).eventStreamReader.getTokenAt(request.getInstant());
            responseObserver.onNext(TrackingToken.newBuilder().setToken(token).build());
            responseObserver.onCompleted();
        }, responseObserver::onError);
    }

    @Override
    public Mono<Long> highestSequenceNumber(String context, String aggregateId) {
        return Mono.create(sink ->
                                   readHighestSequenceNr(context,
                                                         ReadHighestSequenceNrRequest.newBuilder()
                                                                                     .setAggregateId(aggregateId)
                                                                                     .build(),
                                                         new StreamObserver<ReadHighestSequenceNrResponse>() {
                                                             @Override
                                                             public void onNext(
                                                                     ReadHighestSequenceNrResponse readHighestSequenceNrResponse) {
                                                                 sink.success(readHighestSequenceNrResponse.getToSequenceNr());
                                                             }

                                                             @Override
                                                             public void onError(Throwable throwable) {
                                                                 sink.error(throwable);
                                                             }

                                                             @Override
                                                             public void onCompleted() {
                                                                 //nothing to do, already completed
                                                             }
                                                         }));
    }

    private void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                                       StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
        runInDataFetcherPool(() -> {
            long sequenceNumber = workers(context).aggregateReader.readHighestSequenceNr(request.getAggregateId());
            responseObserver.onNext(ReadHighestSequenceNrResponse.newBuilder().setToSequenceNr(sequenceNumber).build());
            responseObserver.onCompleted();
        }, responseObserver::onError);
    }

    public CompletableFuture<Long> getHighestSequenceNr(String context, String aggregateIdenfier, int maxSegmentsHint,
                                                        long maxTokenHint) {
        CompletableFuture<Long> sequenceNr = new CompletableFuture<>();
        runInDataFetcherPool(() -> sequenceNr.complete(
                                     workers(context)
                                             .aggregateReader
                                             .readHighestSequenceNr(aggregateIdenfier, maxSegmentsHint, maxTokenHint))
                , sequenceNr::completeExceptionally);
        return sequenceNr;
    }

    @Override
    public Flux<QueryEventsResponse> queryEvents(String context, Flux<QueryEventsRequest> query,
                                                 Authentication authentication) {
        return Flux.create(sink -> {
            StreamObserver<QueryEventsRequest> requestStream =
                    queryEvents(context,
                                authentication,
                                new StreamObserver<QueryEventsResponse>() {
                                    @Override
                                    public void onNext(QueryEventsResponse queryEventsResponse) {
                                        sink.next(queryEventsResponse);
                                    }

                                    @Override
                                    public void onError(Throwable throwable) {
                                        sink.error(throwable);
                                    }

                                    @Override
                                    public void onCompleted() {
                                        sink.complete();
                                    }
                                });
            query.subscribe(requestStream::onNext, requestStream::onError, requestStream::onCompleted);
        });
    }

    private StreamObserver<QueryEventsRequest> queryEvents(String context, Authentication authentication,
                                                           StreamObserver<QueryEventsResponse> responseObserver) {
        Workers workers = workers(context);
        EventDecorator activeEventDecorator = eventInterceptors
                .noEventReadInterceptors(context) ? eventDecorator : new InterceptorAwareEventDecorator(context,
                                                                                                        authentication);

        return new QueryEventsRequestStreamObserver(workers.eventWriteStorage,
                                                    workers.eventStreamReader,
                                                    workers.aggregateReader,
                                                    defaultLimit,
                                                    timeout,
                                                    activeEventDecorator,
                                                    responseObserver,
                                                    workers.snapshotWriteStorage,
                                                    workers.snapshotStreamReader
        );
    }

    @Override
    public void stop() {
        running = false;
        dataFetcher.shutdown();
        dataWriter.shutdown();
        workersMap.forEach((k, workers) -> workers.close(false));
        try {
            dataWriter.awaitTermination(10, TimeUnit.SECONDS);
            dataFetcher.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // just stop waiting
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    @Scheduled(fixedRateString = "${axoniq.axonserver.event-processor-permits-check:2000}")
    public void checkPermits() {
        workersMap.forEach((k, v) -> v.validateActiveConnections());
    }

    public long getLastEvent(String context) {
        return workers(context).eventStorageEngine.getLastToken();
    }

    public long getLastSnapshot(String context) {
        return workers(context).snapshotStorageEngine.getLastToken();
    }

    /**
     * Creates an iterator to iterate over event transactions, starting at transaction with token fromToken and ending
     * before toToken
     *
     * @param context   The context of the transactions
     * @param fromToken The first transaction token to include
     * @param toToken   Last transaction token (exclusive)
     * @return the iterator
     */
    public CloseableIterator<SerializedTransactionWithToken> eventTransactionsIterator(String context, long fromToken,
                                                                                       long toToken) {
        return workers(context).eventStorageEngine.transactionIterator(fromToken, toToken);
    }

    public CloseableIterator<SerializedEventWithToken> eventIterator(String context, long fromToken) {
        return workers(context).eventStorageEngine.getGlobalIterator(fromToken);
    }

    /**
     * Creates an iterator to iterate over snapshot transactions, starting at transaction with token fromToken and
     * ending before toToken
     *
     * @param context   The context of the transactions
     * @param fromToken The first transaction token to include
     * @param toToken   Last transaction token (exclusive)
     * @return the iterator
     */
    public CloseableIterator<SerializedTransactionWithToken> snapshotTransactionsIterator(String context,
                                                                                          long fromToken,
                                                                                          long toToken) {
        return workersMap.get(context).snapshotStorageEngine.transactionIterator(fromToken, toToken);
    }

    /**
     * todo comment
     * @param context
     * @param token
     * @param segmentVersion the segmentVersion of the segment
     * @param events
     * @return
     */
    public long syncEvents(String context, long token, int segmentVersion, List<Event> events) {
        try {
            Workers worker = workers(context);
            worker.eventSyncStorage.sync(token, segmentVersion, events);
            worker.triggerTrackerEventProcessors();
            return token + events.size();
        } catch (MessagingPlatformException ex) {
            if (ErrorCode.NO_EVENTSTORE.equals(ex.getErrorCode())) {
                logger.warn("{}: cannot store in non-active event store", context);
                return -1;
            } else {
                throw ex;
            }
        }
    }

    public long syncSnapshots(String context, long token, int segmentVersion, List<Event> snapshots) {
        try {
            SyncStorage writeStorage = workers(context).snapshotSyncStorage;
            writeStorage.sync(token, segmentVersion, snapshots);
            return token + snapshots.size();
        } catch (MessagingPlatformException ex) {
            if (ErrorCode.NO_EVENTSTORE.equals(ex.getErrorCode())) {
                logger.warn("{}: cannot store snapshot in non-active event store", context);
                return -1;
            } else {
                throw ex;
            }
        }
    }

    public long getWaitingEventTransactions(String context) {
        return workers(context).eventWriteStorage.waitingTransactions();
    }

    public long getWaitingSnapshotTransactions(String context) {
        return workers(context).snapshotWriteStorage.waitingTransactions();
    }

    public Stream<String> getBackupFilenames(String context, EventType eventType, long lastSegmentBackedUp,
                                             int lastVersionBackedUp, boolean includeActive) {
        try {
            Workers workers = workers(context);
            if (eventType == EventType.SNAPSHOT) {
                return workers.snapshotStorageEngine.getBackupFilenames(lastSegmentBackedUp,
                                                                        lastVersionBackedUp,
                                                                        includeActive);
            }

            return workers.eventStorageEngine.getBackupFilenames(lastSegmentBackedUp,
                                                                 lastVersionBackedUp,
                                                                 includeActive);
        } catch (Exception ex) {
            logger.warn("Failed to get backup filenames", ex);
        }
        return Stream.empty();
    }

    private boolean isClientException(Throwable exception) {
        return exception instanceof MessagingPlatformException
                && ((MessagingPlatformException) exception).getErrorCode().isClientException();
    }

    public long firstToken(String context) {
        return workers(context).eventStorageEngine.getFirstToken();
    }

    public long firstSnapshotToken(String context) {
        return workers(context).snapshotStorageEngine.getFirstToken();
    }

    public boolean activeContext(String context) {
        return workersMap.containsKey(context) && workersMap.get(context).initialized;
    }

    public long getFirstCompletedSegment(String context, EventType type) {
        if( EventType.EVENT.equals(type)) {
            return workers(context).eventStorageEngine.getFirstCompletedSegment();
        }
        return workers(context).snapshotStorageEngine.getFirstCompletedSegment();
    }

    @Override
    public Mono<Void> compact(String context) {
        return workers(context).compact();
    }
    public EventStorageEngine getSnapshotStorage(String context) {
        return workers(context).snapshotStorageEngine;
    }

    private class Workers {

        private final EventWriteStorage eventWriteStorage;
        private final SnapshotWriteStorage snapshotWriteStorage;
        private final AggregateReader aggregateReader;
        private final EventStreamReader eventStreamReader;
        private final EventStreamReader snapshotStreamReader;
        private final EventStorageEngine eventStorageEngine;
        private final EventStorageEngine snapshotStorageEngine;
        private final String context;
        private final SyncStorage eventSyncStorage;
        private final SyncStorage snapshotSyncStorage;
        private final TrackingEventProcessorManager trackingEventManager;
        private final Gauge gauge;
        private final Gauge snapshotGauge;
        private final Object initLock = new Object();
        private volatile boolean initialized;


        public Workers(String context) {
            this.eventStorageEngine = eventStoreFactory.createEventStorageEngine(context);
            this.snapshotStorageEngine = eventStoreFactory.createSnapshotStorageEngine(context);
            this.context = context;
            this.eventWriteStorage = new EventWriteStorage(storageTransactionManagerFactory
                                                                   .createTransactionManager(this.eventStorageEngine));
            this.snapshotWriteStorage = new SnapshotWriteStorage(storageTransactionManagerFactory
                                                                         .createTransactionManager(this.snapshotStorageEngine));
            this.aggregateReader = new AggregateReader(eventStorageEngine, new SnapshotReader(snapshotStorageEngine));
            this.trackingEventManager = new TrackingEventProcessorManager(eventStorageEngine, blacklistedSendAfter);

            this.eventStreamReader = new EventStreamReader(eventStorageEngine);
            this.snapshotStreamReader = new EventStreamReader(snapshotStorageEngine);

            this.snapshotSyncStorage = new SyncStorage(snapshotStorageEngine);
            this.eventSyncStorage = new SyncStorage(eventStorageEngine);
            this.eventWriteStorage.registerEventListener((token, events) -> this.trackingEventManager.reschedule());
            this.gauge = meterFactory.gauge(BaseMetricName.AXON_EVENT_LAST_TOKEN,
                                            Tags.of(MeterFactory.CONTEXT, context),
                                            context,
                                            c -> (double) eventStorageEngine.getLastToken());
            this.snapshotGauge = meterFactory.gauge(BaseMetricName.AXON_SNAPSHOT_LAST_TOKEN,
                                                    Tags.of(MeterFactory.CONTEXT, context),
                                                    context,
                                                    c -> (double) snapshotStorageEngine.getLastToken());
        }

        public void ensureInitialized(boolean validate, long defaultFirstEventIndex, long defaultFirstSnapshotIndex) {
            if (initialized) {
                return;
            }

            synchronized (initLock) {
                if (initialized) {
                    return;
                }
                if (logger.isInfoEnabled()) {
                    logger.info("{}: initializing Workers [{}]", context, System.identityHashCode(this));
                }
                try {
                    eventStorageEngine.init(validate, defaultFirstEventIndex);
                    snapshotStorageEngine.init(validate, defaultFirstSnapshotIndex);
                    initialized = true;

                    if (logger.isInfoEnabled()) {
                        logger.info("Workers[{}] for context {} has been initialized.",
                                    System.identityHashCode(this),
                                    context);
                    }
                } catch (RuntimeException runtimeException) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Problems during initialization of Workers[{}]", System.identityHashCode(this));
                    }
                    eventStorageEngine.close(false);
                    snapshotStorageEngine.close(false);
                    initialized = false;
                    throw runtimeException;
                }
            }
        }

        /**
         * Close all activity on a context and release all resources.
         */
        public void close(boolean deleteData) {
            logger.info("Start closing the Workers[{}] for context {} with deleteData = {}.",
                        System.identityHashCode(this), context, deleteData);
            trackingEventManager.close();
            eventStorageEngine.close(deleteData);
            snapshotStorageEngine.close(deleteData);
            meterFactory.remove(gauge);
            meterFactory.remove(snapshotGauge);
            logger.info("Workers[{}] closed for context {} with deleteData = {}.",
                        System.identityHashCode(this), context, deleteData);
        }

        private TrackingEventProcessorManager.EventTracker createEventTracker(long trackingToken,
                                                                              String clientId,
                                                                              boolean forceReadingFromLeader,
                                                                              StreamObserver<SerializedEventWithToken> eventStream) {
            return trackingEventManager.createEventTracker(trackingToken,
                                                           clientId,
                                                           forceReadingFromLeader,
                                                           eventStream);
        }

        private void triggerTrackerEventProcessors() {
            trackingEventManager.reschedule();
        }


        private void cancelTrackingEventProcessors() {
            trackingEventManager.stopAllWhereNotAllowedReadingFromFollower();
        }

        /**
         * Checks if there are any tracking event processors that are waiting for new permits for a long time. This may
         * indicate that the connection to the client application has gone. If a tracking event processor is waiting too
         * long, the connection will be cancelled and the client needs to restart a tracker.
         */
        private void validateActiveConnections() {
            long minLastPermits = System.currentTimeMillis() - newPermitsTimeout;
            trackingEventManager.validateActiveConnections(minLastPermits);
        }

        public Mono<Void> compact() {
            return eventStorageEngine.deleteOldVersions();
        }
    }

    private class InterceptorAwareEventDecorator implements EventDecorator {

        final DefaultExecutionContext unitOfWork;

        public InterceptorAwareEventDecorator(String context, Authentication authentication) {
            unitOfWork = new DefaultExecutionContext(context, authentication);
        }

        @Override
        public SerializedEvent decorateEvent(SerializedEvent serializedEvent) {
            if (eventInterceptors.noReadInterceptors(unitOfWork.contextName())) {
                return serializedEvent;
            }
            try {
                Event event = serializedEvent.isSnapshot() ?
                        eventInterceptors.readSnapshot(serializedEvent.asEvent(), unitOfWork) :
                        eventInterceptors.readEvent(serializedEvent.asEvent(), unitOfWork);
                return eventDecorator.decorateEvent(new SerializedEvent(event));
            } catch (MessagingPlatformException exception) {
                unitOfWork.compensate(exception);
                throw exception;
            }
        }

        @Override
        public SerializedEventWithToken decorateEventWithToken(SerializedEventWithToken eventWithToken) {
            if (eventInterceptors.noEventReadInterceptors(unitOfWork.contextName())) {
                return eventDecorator.decorateEventWithToken(eventWithToken);
            }

            try {
                Event event = eventInterceptors.readEvent(eventWithToken.asEvent(), unitOfWork);
                return eventDecorator.decorateEventWithToken(new SerializedEventWithToken(eventWithToken.getToken(),
                                                                                          event));
            } catch (RuntimeException exception) {
                unitOfWork.compensate(exception);
                throw exception;
            }
        }

        @Override
        public EventWithToken decorateEventWithToken(EventWithToken event) {
            if (eventInterceptors.noReadInterceptors(unitOfWork.contextName())) {
                return eventDecorator.decorateEventWithToken(event);
            }

            try {
                EventWithToken intercepted = event.getEvent().getSnapshot() ?
                        EventWithToken.newBuilder(event)
                                      .setEvent(eventInterceptors.readSnapshot(event.getEvent(),
                                                                               unitOfWork))
                                      .build() :
                        EventWithToken.newBuilder(event)
                                      .setEvent(eventInterceptors.readEvent(event.getEvent(),
                                                                            unitOfWork))
                                      .build();
                return eventDecorator.decorateEventWithToken(intercepted);
            } catch (RuntimeException exception) {
                unitOfWork.compensate(exception);
                throw exception;
            }
        }
    }
}

