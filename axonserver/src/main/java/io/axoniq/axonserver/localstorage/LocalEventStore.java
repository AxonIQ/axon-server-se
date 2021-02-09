/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ConcurrencyExceptions;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.RequestRejectedException;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
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
import io.axoniq.axonserver.interceptor.DefaultInterceptorContext;
import io.axoniq.axonserver.interceptor.EventInterceptors;
import io.axoniq.axonserver.localstorage.query.QueryEventsRequestStreamObserver;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.system.DiskSpaceHealthIndicatorProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.util.CloseableIterator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Component that handles the actual interaction with the event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Component
public class LocalEventStore implements io.axoniq.axonserver.message.event.EventStore, SmartLifecycle {

    private static final Confirmation CONFIRMATION = Confirmation.newBuilder().setSuccess(true).build();
    private final Logger logger = LoggerFactory.getLogger(LocalEventStore.class);
    private final Map<String, Workers> workersMap = new ConcurrentHashMap<>();
    private final EventStoreFactory eventStoreFactory;
    private final ExecutorService dataFetcher;
    private final ExecutorService dataWriter;
    private final MeterFactory meterFactory;
    private final StorageTransactionManagerFactory storageTransactionManagerFactory;
    private final EventInterceptors eventInterceptors;
    private final int maxEventCount;

    /**
     * Maximum number of blacklisted events to be skipped before it will send a blacklisted event anyway. If almost all
     * events
     * would be ignored due to blacklist, tracking tokens on client applications would never be updated.
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
    @Value("${axoniq.axonserver.check-sequence-nr-for-snapshots:true}")
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
             Short.MAX_VALUE,
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
                           @Value("${axoniq.axonserver.max-events-per-transaction:32767}") int maxEventCount,
                           @Value("${axoniq.axonserver.blacklisted-send-after:1000}") int blacklistedSendAfter,
                           @Value("${axoniq.axonserver.data-fetcher-threads:24}") int fetcherThreads,
                           @Value("${axoniq.axonserver.data-writer-threads:8}") int writerThreads) {
        this.eventStoreFactory = eventStoreFactory;
        this.meterFactory = meterFactory;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
        this.eventInterceptors = eventInterceptors;
        this.maxEventCount = Math.min(maxEventCount, Short.MAX_VALUE);
        this.blacklistedSendAfter = blacklistedSendAfter;
        this.dataFetcher = Executors.newFixedThreadPool(fetcherThreads, new CustomizableThreadFactory("data-fetcher-"));
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
    public void deleteAllEventData(String context) {
        Workers workers = workersMap.get(context);
        if (workers == null) {
            return;
        }
        workers.deleteAllEventData();
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
    public CompletableFuture<Confirmation> appendSnapshot(String context, Authentication authentication,
                                                          Event snapshot) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        runInDataWriterPool(() -> doAppendSnapshot(context, authentication, snapshot, completableFuture),
                            ex -> {
                                logger.warn("{}: Append snapshot failed", context, ex);
                                completableFuture.completeExceptionally(ex);
                            });
        return completableFuture;
    }

    private void doAppendSnapshot(String context, Authentication authentication, Event snapshot,
                                  CompletableFuture<Confirmation> completableFuture) {
        if (checkSequenceNrForSnapshots) {
            long seqNr = workers(context).aggregateReader.readHighestSequenceNr(snapshot.getAggregateIdentifier());
            if (seqNr < snapshot.getAggregateSequenceNumber()) {
                completableFuture.completeExceptionally(new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE,
                                                                                       "Invalid sequence number while storing snapshot"));
                return;
            }
        }
        DefaultInterceptorContext interceptorContext = new DefaultInterceptorContext(context, authentication);
        try {
            Event snapshotAfterInterceptors = eventInterceptors.appendSnapshot(snapshot, interceptorContext);
            workers(context).snapshotWriteStorage.store(snapshotAfterInterceptors)
                                                 .whenComplete(((confirmation, throwable) -> {
                                                     if (throwable != null) {
                                                         interceptorContext.compensate();
                                                         completableFuture.completeExceptionally(throwable);
                                                     } else {
                                                         eventInterceptors.snapshotPostCommit(snapshotAfterInterceptors,
                                                                                              interceptorContext);
                                                         completableFuture.complete(confirmation);
                                                     }
                                                 }));
        } catch (RequestRejectedException requestRejectedException) {
            interceptorContext.compensate();
            completableFuture
                    .completeExceptionally(new MessagingPlatformException(ErrorCode.SNAPSHOT_REJECTED_BY_INTERCEPTOR,
                                                                          "Snapshot rejected by interceptor",
                                                                          requestRejectedException));
        }
    }

    @Override
    public StreamObserver<InputStream> createAppendEventConnection(String context,
                                                                   Authentication authentication,
                                                                   StreamObserver<Confirmation> responseObserver) {
        DefaultInterceptorContext interceptorContext = new DefaultInterceptorContext(context, authentication);
        return new StreamObserver<InputStream>() {
            private final List<Event> eventList = new ArrayList<>();
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public void onNext(InputStream inputStream) {
                if (checkMaxEventCount()) {
                    try {
                        Event event = Event.parseFrom(inputStream);
                        eventList.add(eventInterceptors.appendEvent(event, interceptorContext));
                    } catch (Exception e) {
                        interceptorContext.compensate();
                        responseObserver.onError(GrpcExceptionBuilder.build(e));
                    }
                }
            }

            private boolean checkMaxEventCount() {
                if (eventList.size() < maxEventCount) {
                    return true;
                }
                if (closed.compareAndSet(false, true)) {
                    responseObserver.onError(GrpcExceptionBuilder.build(ErrorCode.TOO_MANY_EVENTS,
                                                                        "Maximum number of events in transaction exceeded: "
                                                                                + maxEventCount));
                }
                return false;
            }

            @Override
            public void onError(Throwable cause) {
                logger.warn("Error on connection to client while storing events", cause);
            }

            @Override
            public void onCompleted() {
                if (closed.get()) {
                    return;
                }
                runInDataWriterPool(() -> {
                    try {
                        eventInterceptors.eventsPreCommit(eventList, interceptorContext);
                        workers(context)
                                .eventWriteStorage
                                .store(eventList)
                                .thenAccept(r -> eventInterceptors.eventsPostCommit(eventList, interceptorContext))
                                .thenRun(this::confirm)
                                .exceptionally(this::error);
                    } catch (RequestRejectedException e) {
                        interceptorContext.compensate();
                        responseObserver.onError(new MessagingPlatformException(ErrorCode.EVENT_REJECTED_BY_INTERCEPTOR,
                                                                                "Event rejected by interceptor",
                                                                                e));
                    }
                }, this::error);
            }

            private Void error(Throwable exception) {
                exception = ConcurrencyExceptions.unwrap(exception);
                if (isClientException(exception)) {
                    logger.info("{}: Error while storing events: {}", context, exception.getMessage());
                } else {
                    logger.warn("{}: Error while storing events", context, exception);
                }
                interceptorContext.compensate();
                responseObserver.onError(exception);
                return null;
            }

            private void confirm() {
                responseObserver.onNext(CONFIRMATION);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void listAggregateEvents(String context, Authentication authentication, GetAggregateEventsRequest request,
                                    StreamObserver<SerializedEvent> responseStreamObserver) {
        runInDataFetcherPool(() -> {
            AtomicInteger counter = new AtomicInteger();
            EventDecorator activeEventDecorator = eventInterceptors.noReadInterceptors(context) ?
                    eventDecorator :
                    new InterceptorAwareEventDecorator(context, authentication);
            workers(context).aggregateReader.readEvents(request.getAggregateId(),
                                                        request.getAllowSnapshots(),
                                                        request.getInitialSequence(),
                                                        getMaxSequence(request),
                                                        request.getMinToken(),
                                                        event -> {
                                                            responseStreamObserver.onNext(activeEventDecorator
                                                                                                  .decorateEvent(event));
                                                            counter.incrementAndGet();
                                                        });
            if (counter.get() == 0) {
                logger.debug("Aggregate not found: {}", request);
            }
            responseStreamObserver.onCompleted();
        }, error -> {
            logger.warn("Problem encountered while reading events for aggregate " + request.getAggregateId(), error);
            responseStreamObserver.onError(error);
        });
    }

    private void runInDataFetcherPool(Runnable task, Consumer<Exception> onError) {
        runInPool(dataFetcher, task, onError);
    }

    private void runInDataWriterPool(Runnable task, Consumer<Exception> onError) {
        runInPool(dataWriter, task, onError);
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
    public void listAggregateSnapshots(String context,
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
    public StreamObserver<GetEventsRequest> listEvents(String context, Authentication authentication,
                                                       StreamObserver<InputStream> responseStreamObserver) {
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
                                                                   new StreamObserver<InputStream>() {
                                                                       @Override
                                                                       public void onNext(InputStream inputStream) {
                                                                           responseStreamObserver.onNext(
                                                                                   activeEventDecorator
                                                                                           .decorateEventWithToken(
                                                                                                   inputStream));
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
    public void getFirstToken(String context, GetFirstTokenRequest request,
                              StreamObserver<TrackingToken> responseObserver) {
        long token = workers(context).eventStreamReader.getFirstToken();
        responseObserver.onNext(TrackingToken.newBuilder().setToken(token).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getLastToken(String context, GetLastTokenRequest request,
                             StreamObserver<TrackingToken> responseObserver) {
        responseObserver.onNext(TrackingToken.newBuilder().setToken(workers(context).eventStorageEngine.getLastToken())
                                             .build());
        responseObserver.onCompleted();
    }

    public void getLastSnapshotToken(String context,
                                     StreamObserver<TrackingToken> responseObserver) {
        responseObserver.onNext(TrackingToken.newBuilder()
                                             .setToken(workers(context).snapshotStorageEngine.getLastToken()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver) {
        runInDataFetcherPool(() -> {
            long token = workers(context).eventStreamReader.getTokenAt(request.getInstant());
            responseObserver.onNext(TrackingToken.newBuilder().setToken(token).build());
            responseObserver.onCompleted();
        }, responseObserver::onError);
    }

    @Override
    public void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
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
    public StreamObserver<QueryEventsRequest> queryEvents(String context, Authentication authentication,
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
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(@Nonnull Runnable runnable) {
        running = false;
        dataFetcher.shutdown();
        workersMap.forEach((k, workers) -> workers.close(false));
        try {
            dataFetcher.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // just stop waiting
            Thread.currentThread().interrupt();
        }
        dataFetcher.shutdownNow();
        runnable.run();
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        stop(() -> {
        });
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
        return workersMap.get(context).eventStorageEngine.transactionIterator(fromToken, toToken);
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

    public long syncEvents(String context, SerializedTransactionWithToken value) {
        try {
            Workers worker = workers(context);
            worker.eventSyncStorage.sync(value.getToken(), value.getEvents());
            worker.triggerTrackerEventProcessors();
            return value.getToken() + value.getEvents().size();
        } catch (MessagingPlatformException ex) {
            if (ErrorCode.NO_EVENTSTORE.equals(ex.getErrorCode())) {
                logger.warn("{}: cannot store in non-active event store", context);
                return -1;
            } else {
                throw ex;
            }
        }
    }

    public long syncSnapshots(String context, SerializedTransactionWithToken value) {
        try {
            SyncStorage writeStorage = workers(context).snapshotSyncStorage;
            writeStorage.sync(value.getToken(), value.getEvents());
            return value.getToken() + value.getEvents().size();
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

    public Stream<String> getBackupFilenames(String context, EventType eventType, long lastSegmentBackedUp) {
        try {
            Workers workers = workers(context);
            if (eventType == EventType.SNAPSHOT) {
                return workers.snapshotStorageEngine.getBackupFilenames(lastSegmentBackedUp);
            }

            return workers.eventStorageEngine.getBackupFilenames(lastSegmentBackedUp);
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

    public boolean hasContext(String context) {
        return workersMap.containsKey(context);
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
                logger.debug("{}: initializing", context);
                try {
                    eventStorageEngine.init(validate, defaultFirstEventIndex);
                    snapshotStorageEngine.init(validate, defaultFirstSnapshotIndex);
                    initialized = true;
                } catch (RuntimeException runtimeException) {
                    eventStorageEngine.close(false);
                    snapshotStorageEngine.close(false);
                    throw runtimeException;
                }


            }
        }

        /**
         * Close all activity on a context and release all resources.
         */
        public void close(boolean deleteData) {
            trackingEventManager.close();
            eventStorageEngine.close(deleteData);
            snapshotStorageEngine.close(deleteData);
            meterFactory.remove(gauge);
            meterFactory.remove(snapshotGauge);
        }

        private TrackingEventProcessorManager.EventTracker createEventTracker(long trackingToken,
                                                                              String clientId,
                                                                              boolean forceReadingFromLeader,
                                                                              StreamObserver<InputStream> eventStream) {
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
         * Checks if there are any tracking event processors that are waiting for new permits for a long time.
         * This may indicate that the connection to the client application has gone. If a tracking event processor
         * is waiting too long, the connection will be cancelled and the client needs to restart a tracker.
         */
        private void validateActiveConnections() {
            long minLastPermits = System.currentTimeMillis() - newPermitsTimeout;
            trackingEventManager.validateActiveConnections(minLastPermits);
        }

        /**
         * Deletes all event and snapshot data for this context.
         */
        public void deleteAllEventData() {
            eventWriteStorage.deleteAllEventData();
            snapshotWriteStorage.deleteAllEventData();
        }

    }

    private class InterceptorAwareEventDecorator implements EventDecorator {

        final ExtensionUnitOfWork extensionUnitOfWork;

        public InterceptorAwareEventDecorator(String context, Authentication authentication) {
            extensionUnitOfWork = new DefaultInterceptorContext(context, authentication);
        }

        @Override
        public SerializedEvent decorateEvent(SerializedEvent serializedEvent) {
            Event event = serializedEvent.isSnapshot() ?
                    eventInterceptors.readSnapshot(serializedEvent.asEvent(), extensionUnitOfWork) :
                    eventInterceptors.readEvent(serializedEvent.asEvent(), extensionUnitOfWork);
            return eventDecorator.decorateEvent(new SerializedEvent(event));
        }

        @Override
        public InputStream decorateEventWithToken(InputStream inputStream) {
            if (eventInterceptors.noEventReadInterceptors(extensionUnitOfWork.context())) {
                return eventDecorator.decorateEventWithToken(inputStream);
            }

            try {
                EventWithToken eventWithToken = EventWithToken.parseFrom(inputStream);
                Event event = eventInterceptors.readEvent(eventWithToken.getEvent(), extensionUnitOfWork);
                return eventDecorator.decorateEventWithToken(new SerializedEventWithToken(eventWithToken.getToken(),
                                                                                          event)
                                                                     .asInputStream());
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        public EventWithToken decorateEventWithToken(EventWithToken event) {
            if (eventInterceptors.noReadInterceptors(extensionUnitOfWork.context())) {
                return eventDecorator.decorateEventWithToken(event);
            }

            EventWithToken intercepted = event.getEvent().getSnapshot() ?
                    EventWithToken.newBuilder(event)
                                  .setEvent(eventInterceptors.readSnapshot(event.getEvent(),
                                                                           extensionUnitOfWork))
                                  .build() :
                    EventWithToken.newBuilder(event)
                                  .setEvent(eventInterceptors.readEvent(event.getEvent(),
                                                                        extensionUnitOfWork))
                                  .build();
            return eventDecorator.decorateEventWithToken(intercepted);
        }
    }
}

