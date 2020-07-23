package io.axoniq.axonserver.enterprise.storage.transaction;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.transaction.SequenceNumberCache;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Implemementation of {@link StorageTransactionManager} that replicates data using the raft protocol
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class RaftTransactionManager implements StorageTransactionManager {
    private final Logger logger = LoggerFactory.getLogger(RaftTransactionManager.class);
    private final EventStorageEngine eventStorageEngine;
    private final GrpcRaftController clusterController;
    private final AtomicLong nextTransactionToken = new AtomicLong();
    private final AtomicInteger waitingTransactions = new AtomicInteger();
    private final boolean eventTransactionManager;
    private final String entryType;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final ReentrantLock lock = new ReentrantLock();
    private final SequenceNumberCache sequenceNumberCache;


    public RaftTransactionManager(EventStorageEngine eventStorageEngine,
                                  GrpcRaftController clusterController,
                                  MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.eventStorageEngine = eventStorageEngine;
        this.clusterController = clusterController;
        this.eventTransactionManager = eventStorageEngine.getType().getEventType().equals(EventType.EVENT);
        this.entryType = "Append." + eventStorageEngine.getType().getEventType();
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.sequenceNumberCache = new SequenceNumberCache(eventStorageEngine::getLastSequenceNumber);
        eventStorageEngine.registerCloseListener(this.sequenceNumberCache::close);
    }

    public void on(ClusterEvents.BecomeContextLeader becomeMaster) {
        lock.lock();
        try (EntryIterator iterator = becomeMaster.unappliedEntriesSupplier().get()) {
            sequenceNumberCache.clear();
            waitingTransactions.set(0);
            nextTransactionToken.set(eventStorageEngine.getLastToken() + 1);
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                if (forMe(entry)) {
                    try {
                        TransactionWithToken transactionWithToken = TransactionWithToken
                                .parseFrom(entry.getSerializedObject().getData());
                        if (logger.isDebugEnabled()) {
                            logger.debug("{}: found {} transaction {}, expected {}", eventStorageEngine.getType(),
                                         entry.getSerializedObject().getType(),
                                         transactionWithToken.getToken(),
                                         nextTransactionToken.get());
                        }
                        if (transactionWithToken.getToken() >= nextTransactionToken.get()) {

                            List<SerializedEvent> serializedEvents = transactionWithToken.getEventsList()
                                                                                         .stream()
                                                                                         .map(bytes -> new SerializedEvent(
                                                                                                 bytes.toByteArray()))
                                                                                         .collect(Collectors
                                                                                                          .toList());
                            if (logger.isInfoEnabled()) {
                                serializedEvents.forEach(e -> logger
                                        .info("{}/{} reserve sequence numbers for {} : {}",
                                              entry.getTerm(),
                                              entry.getIndex(),
                                              e.getAggregateIdentifier(),
                                              e.getAggregateSequenceNumber()));
                            }
                            if (eventTransactionManager) {
                                sequenceNumberCache.reserveSequenceNumbers(serializedEvents, true);
                            }
                            nextTransactionToken.addAndGet(transactionWithToken.getEventsCount());
                        }
                    } catch (Exception e) {
                        throw new MessagingPlatformException(ErrorCode.OTHER,
                                                             "Error while preparing transaction manager for "
                                                                     + eventStorageEngine.getType(),
                                                             e);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean forMe(Entry entry) {
        return entry.hasSerializedObject() && entry.getSerializedObject().getType().equals(entryType);
    }

    public void on(ClusterEvents.ContextLeaderStepDown masterStepDown) {
        nextTransactionToken.set(-1);
        logger.info("{}: Step down", eventStorageEngine.getType());
        sequenceNumberCache.clear();
    }

    @Override
    public CompletableFuture<Long> store(List<SerializedEvent> eventList) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            Iterable<? extends ByteString> events = eventsAsByteStrings(eventList);
            // Ensure that only one thread is generating transaction token and log entry index at the same time

            CompletableFuture<Void> appendEntryResult;
            TransactionWithToken transactionWithToken;
            lock.lock();
            try {
                RaftNode node = clusterController.getRaftNodeForContext(eventStorageEngine.getType().getContext());
                if( node == null || !node.isLeader()) {
                    completableFuture
                            .completeExceptionally(new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE,
                                                                                  "No longer leader"));
                    return completableFuture;
                }

                transactionWithToken =
                        TransactionWithToken.newBuilder().setToken(nextTransactionToken.getAndAdd(eventList.size()))
                                            .setVersion(eventStorageEngine.transactionVersion())
                                            .setContext(eventStorageEngine.getType().getContext())
                                            .addAllEvents(events).build();

                if( logger.isTraceEnabled()) {
                    logger.trace("Append transaction: {} with {} events",
                                 transactionWithToken.getToken(),
                                 transactionWithToken.getEventsCount());
                }
                appendEntryResult = node.appendEntry(entryType, transactionWithToken.toByteArray());
                waitingTransactions.incrementAndGet();
            } finally {
                lock.unlock();
            }
            //
            appendEntryResult.whenComplete((r, cause) -> {
                waitingTransactions.decrementAndGet();
                if (cause == null) {
                    completableFuture.complete(transactionWithToken.getToken());
                } else {
                    completableFuture.completeExceptionally(cause);
                }
            });
        } catch( Exception ex) {
            logger.warn("Failed to store", ex);
            completableFuture.completeExceptionally(ex);
        }
        return completableFuture;
    }

    private Iterable<? extends ByteString> eventsAsByteStrings(List<SerializedEvent> eventList) {
        AtomicInteger totalSize = new AtomicInteger();
        List<ByteString> result = eventList.stream()
                                           .map(SerializedEvent::asByteString)
                                           .peek(byteString -> totalSize.addAndGet(byteString.size()))
                                           .collect(Collectors.toList());
        if (totalSize.get() > messagingPlatformConfiguration.getMaxTransactionSize()) {
            throw new MessagingPlatformException(ErrorCode.VALIDATION_FAILED,
                                                 String.format("Transaction size %d exceeds %d", totalSize.get(), messagingPlatformConfiguration.getMaxTransactionSize()));
        }
        return result;
    }


    @Override
    public Runnable reserveSequenceNumbers(List<SerializedEvent> eventList) {
        return sequenceNumberCache.reserveSequenceNumbers(eventList, false);
    }

    @Override
    public long waitingTransactions() {
        return waitingTransactions.get();
    }

    @Override
    public void deleteAllEventData() {
        sequenceNumberCache.clear();
        eventStorageEngine.deleteAllEventData();
    }

}
