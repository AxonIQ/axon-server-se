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
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
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
import org.springframework.boot.actuate.health.Health;
import org.springframework.context.SmartLifecycle;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Component that handles the actual interaction with the event store.
 * @author Marc Gathier
 * @since 4.0
 */
@Component
public class LocalEventStore implements io.axoniq.axonserver.message.event.EventStore, SmartLifecycle {
    private static final Confirmation CONFIRMATION = Confirmation.newBuilder().setSuccess(true).build();
    private static final boolean CREATE_IF_MISSING = true;
    private final Logger logger = LoggerFactory.getLogger(LocalEventStore.class);
    private final Map<String, Workers> workersMap = new ConcurrentHashMap<>();
    private final EventStoreFactory eventStoreFactory;
    private final ExecutorService dataFetcher;
    private volatile boolean running;
    @Value("${axoniq.axonserver.query.limit:200}")
    private long defaultLimit = 200;
    @Value("${axoniq.axonserver.query.timeout:300000}")
    private long timeout = 300000;
    @Value("${axoniq.axonserver.new-permits-timeout:120000}")
    private long newPermitsTimeout=120000;

    private final MeterFactory meterFactory;
    private final StorageTransactionManagerFactory storageTransactionManagerFactory;
    private final EventStoreExistChecker eventStoreExistChecker;
    private final int maxEventCount;

    /**
     * Maximum number of blacklisted events to be skipped before it will send a blacklisted event anyway. If almost all events
     * would be ignored due to blacklist, tracking tokens on client applications would never be updated.
     */
    private final int blacklistedSendAfter;

    public LocalEventStore(EventStoreFactory eventStoreFactory, MeterRegistry meterFactory,
                           StorageTransactionManagerFactory storageTransactionManagerFactory,
                           EventStoreExistChecker eventStoreExistChecker) {
        this(eventStoreFactory,
             new MeterFactory(meterFactory, new DefaultMetricCollector()),
             storageTransactionManagerFactory,
             eventStoreExistChecker, Short.MAX_VALUE,
             1000,
             24);
    }

    @Autowired
    public LocalEventStore(EventStoreFactory eventStoreFactory,
                           MeterFactory meterFactory,
                           StorageTransactionManagerFactory storageTransactionManagerFactory,
                           EventStoreExistChecker eventStoreExistChecker, @Value("${axoniq.axonserver.max-events-per-transaction:32767}") int maxEventCount,
                           @Value("${axoniq.axonserver.blacklisted-send-after:1000}") int blacklistedSendAfter,
                           @Value("${axoniq.axonserver.data-fetcher-threads:24}") int fetcherThreads) {
        this.eventStoreFactory = eventStoreFactory;
        this.meterFactory = meterFactory;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
        this.eventStoreExistChecker = eventStoreExistChecker;
        this.maxEventCount = Math.min(maxEventCount, Short.MAX_VALUE);
        this.blacklistedSendAfter = blacklistedSendAfter;
        this.dataFetcher = Executors.newFixedThreadPool(fetcherThreads, new CustomizableThreadFactory("data-fetcher-"));
    }

    public void initContext(String context, boolean validating) {
        if( workersMap.containsKey(context)) return;
        workersMap.putIfAbsent(context, new Workers(context));
        workersMap.get(context).init(validating);
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
        if( workers == null) return;
        workers.close(!keepData);
    }

    /**
     * Deletes all event data from the context. Context remains alive.
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
        if( workers == null) return;

        workers.eventWriteStorage.cancelPendingTransactions();
        workers.cancelTrackingEventProcessors();
    }

    private Workers workers(String context) {
        return workers(context, false);
    }

    private Workers workers(String context, boolean createIfMissing) {
        Workers workers = workersMap.get(context);
        if (workers == null) {
            if (createIfMissing) {
                synchronized (workersMap) {
                    initContext(context, false);
                    workers = workersMap.get(context);
                }
            } else {
                throw new MessagingPlatformException(ErrorCode.NO_EVENTSTORE, "Missing worker for context: " + context);
            }
        }
        return workers;
    }
    @Override
    public CompletableFuture<Confirmation> appendSnapshot(String context, Event eventMessage) {
        return workers(context).snapshotWriteStorage.store( eventMessage);
    }

    @Override
    public StreamObserver<InputStream> createAppendEventConnection(String context,
                                                                   StreamObserver<Confirmation> responseObserver) {
        return new StreamObserver<InputStream>() {
            private final List<SerializedEvent> eventList = new ArrayList<>();
            private final AtomicBoolean closed = new AtomicBoolean();
            @Override
            public void onNext(InputStream event) {
                if (checkMaxEventCount()) {
                    try {
                        eventList.add(new SerializedEvent(event));
                    } catch (Exception e) {
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
                if( closed.get()) return;

                workers(context)
                        .eventWriteStorage
                        .store(eventList)
                        .thenRun(this::confirm)
                        .exceptionally(this::error);
            }

            private Void error(Throwable exception) {
                exception = ConcurrencyExceptions.unwrap(exception);
                if( isClientException(exception)) {
                    logger.info("{}: Error while storing events: {}", context, exception.getMessage());
                } else {
                    logger.warn("{}: Error while storing events", context, exception);
                }
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
    public void listAggregateEvents(String context, GetAggregateEventsRequest request,
                                    StreamObserver<InputStream> responseStreamObserver) {
        runInDataFetcherPool(() -> {
            AtomicInteger counter = new AtomicInteger();
            workers(context).aggregateReader.readEvents(request.getAggregateId(),
                                                        request.getAllowSnapshots(),
                                                        request.getInitialSequence(),
                                                        event -> {
                                                            responseStreamObserver.onNext(event.asInputStream());
                                                            counter.incrementAndGet();
                                                        });
            if (counter.get() == 0) {
                logger.debug("Aggregate not found: {}", request);
            }
            responseStreamObserver.onCompleted();
        }, responseStreamObserver::onError);
    }

    private void runInDataFetcherPool(Runnable task, Consumer<Exception> onError) {
        try {
            dataFetcher.submit(() -> {
                // we will not start new reader tasks when a shutdown was initialized. Behavior is similar to submitting tasks after shutdown
                if (!running) {
                    onError.accept(new RejectedExecutionException("Cannot load events. AxonServer is shutting down"));
                } else {
                    task.run();
                }
            });
        } catch (Exception e) {
            // in case we didn't manage to schedule the task to run.
            onError.accept(e);
        }
    }

    @Override
    public void listAggregateSnapshots(String context, GetAggregateSnapshotsRequest request,
                                       StreamObserver<InputStream> responseStreamObserver) {
        runInDataFetcherPool(() -> {
            if (request.getMaxSequence() >= 0) {
                workers(context).aggregateReader.readSnapshots(request.getAggregateId(),
                                                               request.getInitialSequence(),
                                                               request.getMaxSequence(),
                                                               request.getMaxResults(),
                                                               event -> responseStreamObserver
                                                                       .onNext(event.asInputStream()));
            }
            responseStreamObserver.onCompleted();
        }, responseStreamObserver::onError);
    }

    @Override
    public StreamObserver<GetEventsRequest> listEvents(String context,
                                                       StreamObserver<InputStream> responseStreamObserver) {
        return new StreamObserver<GetEventsRequest>() {
            private volatile TrackingEventProcessorManager.EventTracker controller;
            @Override
            public void onNext(GetEventsRequest getEventsRequest) {
                if( controller == null) {
                    controller = workers(context).createEventTracker(getEventsRequest,responseStreamObserver);
                } else {
                    controller.addPermits((int) getEventsRequest.getNumberOfPermits());
                    if (getEventsRequest.getBlacklistCount() > 0) {
                        controller.addBlacklist(getEventsRequest.getBlacklistList());
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if( controller != null) controller.close();
                StreamObserverUtils.complete(responseStreamObserver);
            }

            @Override
            public void onCompleted() {
                if( controller != null) controller.close();
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
        responseObserver.onNext(TrackingToken.newBuilder().setToken(workers(context).eventStorageEngine.getLastToken()).build());
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

    @Override
    public StreamObserver<QueryEventsRequest> queryEvents(String context,
                                                          StreamObserver<QueryEventsResponse> responseObserver) {
        Workers workers = workers(context);
        return new QueryEventsRequestStreamObserver(workers.eventWriteStorage, workers.eventStreamReader, defaultLimit, timeout, responseObserver);
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(@NonNull Runnable runnable) {
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
        stop(() -> {});
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

    public long getLastToken(String context) {
        workersMap.computeIfAbsent(context, this::openIfExist);
        return workers(context).eventStorageEngine.getLastToken();
    }

    private Workers openIfExist(String context) {
        if (this.eventStoreExistChecker.exists(context)) {
            Workers workers = new Workers(context);
            workers.init(false);
            return workers;
        }
        return null;
    }

    public long getLastSnapshot(String context) {
        workersMap.computeIfAbsent(context, this::openIfExist);
        return workers(context).snapshotStorageEngine.getLastToken();
    }

    /**
     * Creates an iterator to iterate over event transactions, starting at transaction with token fromToken and ending before toToken
     *
     * @param context The context of the transactions
     * @param fromToken The first transaction token to include
     * @param toToken Last transaction token (exclusive)
     * @return the iterator
     */
    public Iterator<SerializedTransactionWithToken> eventTransactionsIterator(String context, long fromToken, long toToken) {
        return workersMap.get(context).eventStorageEngine.transactionIterator(fromToken, toToken);
    }

    /**
     * Creates an iterator to iterate over snapshot transactions, starting at transaction with token fromToken and ending before toToken
     *
     * @param context The context of the transactions
     * @param fromToken The first transaction token to include
     * @param toToken Last transaction token (exclusive)
     * @return the iterator
     */
    public Iterator<SerializedTransactionWithToken> snapshotTransactionsIterator(String context, long fromToken, long toToken) {
        return workersMap.get(context).snapshotStorageEngine.transactionIterator(fromToken, toToken);
    }

    public long syncEvents(String context, SerializedTransactionWithToken value) {
        SyncStorage writeStorage = workers(context, CREATE_IF_MISSING).eventSyncStorage;
        writeStorage.sync(value.getToken(), value.getEvents());
        return value.getToken() + value.getEvents().size();
    }

    public long syncSnapshots(String context, SerializedTransactionWithToken value) {
        SyncStorage writeStorage = workers(context, CREATE_IF_MISSING).snapshotSyncStorage;
        writeStorage.sync(value.getToken(), value.getEvents());
        return value.getToken() + value.getEvents().size();
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
        } catch (Exception ex ) {
            logger.warn("Failed to get backup filenames", ex);
        }
        return Stream.empty();
    }

    public void health(Health.Builder builder) {
        workersMap.values().forEach(worker -> worker.eventStreamReader.health(builder));
    }

    private boolean isClientException(Throwable exception) {
        return exception instanceof MessagingPlatformException
                && ((MessagingPlatformException) exception).getErrorCode().isClientException();
    }

    private class Workers {
        private final EventWriteStorage eventWriteStorage;
        private final SnapshotWriteStorage snapshotWriteStorage;
        private final AggregateReader aggregateReader;
        private final EventStreamReader eventStreamReader;
        private final EventStorageEngine eventStorageEngine;
        private final EventStorageEngine snapshotStorageEngine;
        private final String context;
        private final SyncStorage eventSyncStorage;
        private final SyncStorage snapshotSyncStorage;
        private final AtomicBoolean initialized = new AtomicBoolean();
        private final TrackingEventProcessorManager trackingEventManager;
        private final Gauge gauge;


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
            this.snapshotSyncStorage = new SyncStorage(snapshotStorageEngine);
            this.eventSyncStorage = new SyncStorage(eventStorageEngine);
            this.eventWriteStorage.registerEventListener((token, events) -> this.trackingEventManager.reschedule());
            this.gauge = meterFactory.gauge(BaseMetricName.AXON_EVENT_LAST_TOKEN,
                                            Tags.of(MeterFactory.CONTEXT, context),
                                            context,
                                            c -> (double) getLastToken(c));

        }

        public synchronized void init(boolean validate) {
            logger.debug("{}: init called", context);
            if( initialized.compareAndSet(false, true)) {
                logger.debug("{}: initializing", context);
                eventStorageEngine.init(validate);
                snapshotStorageEngine.init(validate);
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
        }

        private TrackingEventProcessorManager.EventTracker createEventTracker(GetEventsRequest request, StreamObserver<InputStream> eventStream) {
            return trackingEventManager.createEventTracker(request, eventStream);
        }


        private void cancelTrackingEventProcessors() {
            trackingEventManager.stopAll();
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
}

