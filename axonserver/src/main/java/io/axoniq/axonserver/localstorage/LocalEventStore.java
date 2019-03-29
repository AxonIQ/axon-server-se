package io.axoniq.axonserver.localstorage;

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
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.context.SmartLifecycle;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Component that handles the actual interaction with the event store
 * @author Marc Gathier
 */
@Component
public class LocalEventStore implements io.axoniq.axonserver.message.event.EventStore, SmartLifecycle {
    private static final Confirmation CONFIRMATION = Confirmation.newBuilder().setSuccess(true).build();
    private final Logger logger = LoggerFactory.getLogger(LocalEventStore.class);
    private final Map<String, Workers> workersMap = new ConcurrentHashMap<>();
    private final EventStoreFactory eventStoreFactory;
    private volatile boolean running;
    @Value("${axoniq.axonserver.query.limit:200}")
    private long defaultLimit = 200;
    @Value("${axoniq.axonserver.query.timeout:300000}")
    private long timeout = 300000;
    @Value("${axoniq.axonserver.new-permits-timeout:120000}")
    private long newPermitsTimeout=120000;

    private final int maxEventCount;

    public LocalEventStore(EventStoreFactory eventStoreFactory) {
        this(eventStoreFactory, Short.MAX_VALUE);
    }

    @Autowired
    public LocalEventStore(EventStoreFactory eventStoreFactory, @Value("${axoniq.axonserver.max-events-per-transaction:32767}") int maxEventCount) {
        this.eventStoreFactory = eventStoreFactory;
        this.maxEventCount = Math.min(maxEventCount, Short.MAX_VALUE);
    }

    public void initContext(String context, boolean validating) {
        if( workersMap.containsKey(context)) return;
        workersMap.putIfAbsent(context, new Workers(context));
        workersMap.get(context).init(validating);
    }

    public void cleanupContext(String context) {
        Workers workers = workersMap.remove(context);
        if( workers == null) return;
        workers.cleanup();
    }

    @Override
    public void deleteAllEventData(String context) {
        Workers workers = workersMap.get(context);
        if (workers == null) {
            return;
        }
        workers.eventDatafileManagerChain.deleteAllEventData();
        workers.snapshotDatafileManagerChain.deleteAllEventData();
    }

    public void cancel(String context) {
        Workers workers = workersMap.get(context);
        if( workers == null) return;

        workers.eventWriteStorage.cancelPendingTransactions();
        workers.cancelTrackingEventProcessors();
    }

    private Workers workers(String context) {
        Workers workers = workersMap.get(context);
        if( workers == null) throw new MessagingPlatformException(ErrorCode.NO_EVENTSTORE, "Missing worker for context: " + context);
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
                if( isClientException(exception)) {
                    logger.warn("Error while storing events: {}", exception.getMessage());
                } else {
                    logger.warn("Error while storing events", exception);
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
        AtomicInteger counter = new AtomicInteger();
        workers(context).aggregateReader.readEvents( request.getAggregateId(),
                                                            request.getAllowSnapshots(),
                                                            request.getInitialSequence(),
                                                            event -> {
                                                                responseStreamObserver.onNext(event.asInputStream());
                                                                counter.incrementAndGet();
                                                            });
        if( counter.get() == 0) {
            logger.error("Aggregate not found: {}", request);
        }
        responseStreamObserver.onCompleted();
    }

    @Override
    public void listAggregateSnapshots(String context, GetAggregateSnapshotsRequest request,
                                    StreamObserver<InputStream> responseStreamObserver) {
        if( request.getMaxSequence() >= 0) {
            workers(context).aggregateReader.readSnapshots(request.getAggregateId(),
                                                                  request.getInitialSequence(),
                                                                  request.getMaxSequence(),
                                                                  request.getMaxResults(),
                                                                  event -> responseStreamObserver
                                                                          .onNext(event.asInputStream()));
        }
        responseStreamObserver.onCompleted();
    }

    @Override
    public StreamObserver<GetEventsRequest> listEvents(String context,
                                                       StreamObserver<InputStream> responseStreamObserver) {
        EventStreamController controller = workers(context).createController(
                eventWithToken -> responseStreamObserver.onNext(eventWithToken.asInputStream()),responseStreamObserver::onError);
        return new StreamObserver<GetEventsRequest>() {
            @Override
            public void onNext(GetEventsRequest getEventsRequest) {
                controller.update(getEventsRequest.getTrackingToken(), getEventsRequest.getNumberOfPermits());
            }

            @Override
            public void onError(Throwable throwable) {
                if( workersMap.containsKey(context))
                    workersMap.get(context).stop(controller);

            }

            @Override
            public void onCompleted() {
                if( workersMap.containsKey(context))
                    workersMap.get(context).stop(controller);
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
        responseObserver.onNext(TrackingToken.newBuilder().setToken(workers(context).eventWriteStorage.getLastToken()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver) {
        long token = workers(context).eventStreamReader.getTokenAt(request.getInstant());
        responseObserver.onNext(TrackingToken.newBuilder().setToken(token).build());
        responseObserver.onCompleted();
    }

    @Override
    public void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
            long sequenceNumber = workers(context).aggregateReader.readHighestSequenceNr(request.getAggregateId());
            responseObserver.onNext(ReadHighestSequenceNrResponse.newBuilder().setToSequenceNr(sequenceNumber).build());
            responseObserver.onCompleted();
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
        workersMap.forEach((k, workers) -> workers.cleanup());
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
        return workers(context).eventWriteStorage.getLastToken();
    }

    public long getLastSnapshot(String context) {
        return workers(context).snapshotWriteStorage.getLastToken();
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
        return workersMap.get(context).eventStreamReader.transactionIterator(fromToken, toToken);
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
        return workersMap.get(context).snapshotStreamReader.transactionIterator(fromToken, toToken);
    }

    public long syncEvents(String context, SerializedTransactionWithToken value) {
        SyncStorage writeStorage = workers(context).eventSyncStorage;
        writeStorage.sync(value.getToken(), value.getEvents());
        return value.getToken() + value.getEvents().size();
    }

    public long syncSnapshots(String context, SerializedTransactionWithToken value) {
        SyncStorage writeStorage = workers(context).snapshotSyncStorage;
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
                return workers.snapshotDatafileManagerChain.getBackupFilenames(lastSegmentBackedUp);
            }

            return workers.eventDatafileManagerChain.getBackupFilenames(lastSegmentBackedUp);
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

    Set<EventStreamController> eventStreamControllers(String context) {
        return workers(context).eventStreamControllers();
    }


    private class Workers {
        private final EventWriteStorage eventWriteStorage;
        private final SnapshotWriteStorage snapshotWriteStorage;
        private final AggregateReader aggregateReader;
        private final EventStreamReader eventStreamReader;
        private final EventStreamReader snapshotStreamReader;
        private final EventStore eventDatafileManagerChain;
        private final EventStore snapshotDatafileManagerChain;
        private final String context;
        private final SyncStorage eventSyncStorage;
        private final SyncStorage snapshotSyncStorage;
        private final AtomicBoolean initialized = new AtomicBoolean();
        private final Set<EventStreamController> eventStreamControllerSet = new CopyOnWriteArraySet<>();

        public Workers(String context) {
            this.eventDatafileManagerChain = eventStoreFactory.createEventManagerChain(context);
            this.snapshotDatafileManagerChain = eventStoreFactory.createSnapshotManagerChain(context);
            this.context = context;
            this.eventWriteStorage = new EventWriteStorage(eventStoreFactory.createTransactionManager(this.eventDatafileManagerChain));
            this.snapshotWriteStorage = new SnapshotWriteStorage(eventStoreFactory.createTransactionManager(this.snapshotDatafileManagerChain));
            this.aggregateReader = new AggregateReader(eventDatafileManagerChain, new SnapshotReader(snapshotDatafileManagerChain));
            this.eventStreamReader = new EventStreamReader(eventDatafileManagerChain, eventWriteStorage);
            this.snapshotStreamReader = new EventStreamReader(snapshotDatafileManagerChain, null);
            this.snapshotSyncStorage = new SyncStorage(snapshotDatafileManagerChain);
            this.eventSyncStorage = new SyncStorage(eventDatafileManagerChain);
        }

        public synchronized void init(boolean validate) {
            logger.debug("{}: init called", context);
            if( initialized.compareAndSet(false, true)) {
                logger.debug("{}: initializing", context);
                eventDatafileManagerChain.init(validate);
                snapshotDatafileManagerChain.init(validate);
            }
        }

        public void cleanup() {
            eventDatafileManagerChain.cleanup();
            snapshotDatafileManagerChain.cleanup();
            cancelTrackingEventProcessors();
        }

        private EventStreamController createController(Consumer<SerializedEventWithToken> consumer, Consumer<Throwable> errorCallback) {
            EventStreamController controller = eventStreamReader.createController(consumer, errorCallback);
            eventStreamControllerSet.add(controller);
            return controller;
        }

        public void stop(EventStreamController controller) {
            eventStreamControllerSet.remove(controller);
            controller.stop();
        }

        private void cancelTrackingEventProcessors() {
            eventStreamControllerSet.forEach(EventStreamController::cancel);
            eventStreamControllerSet.clear();
        }

        private void validateActiveConnections() {
            long minLastPermits = System.currentTimeMillis() - newPermitsTimeout;
            eventStreamControllerSet.forEach(controller -> {
                    if( controller.missingNewPermits(minLastPermits)) {
                        logger.warn("{}: Closing connection as waiting for new permits", context) ;
                        controller.cancel();
                        eventStreamControllerSet.remove(controller);
                    }
            });
        }

        Set<EventStreamController> eventStreamControllers() {
            return eventStreamControllerSet;
        }
    }
}

