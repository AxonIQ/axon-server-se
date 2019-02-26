package io.axoniq.axonserver.enterprise.storage.transaction;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.SerializedEvent;
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
 * Author: marc
 */
public class RaftTransactionManager implements StorageTransactionManager {
    private final Logger logger = LoggerFactory.getLogger(RaftTransactionManager.class);
    private final EventStore datafileManagerChain;
    private final GrpcRaftController clusterController;
    private final AtomicLong nextTransactionToken = new AtomicLong();
    private final AtomicInteger waitingTransactions = new AtomicInteger();
    private final boolean eventTransactionManager;
    private final String entryType;
    private final ReentrantLock lock = new ReentrantLock();


    public RaftTransactionManager(EventStore datafileManagerChain,
                                  GrpcRaftController clusterController) {
        this.datafileManagerChain = datafileManagerChain;
        this.clusterController = clusterController;
        this.eventTransactionManager = datafileManagerChain.getType().getEventType().equals(EventType.EVENT);
        this.entryType = "Append." + datafileManagerChain.getType().getEventType();
    }

    public void on(ClusterEvents.BecomeLeader becomeMaster) {
        waitingTransactions.set(0);
        nextTransactionToken.set(datafileManagerChain.getLastToken()+1);
            try (EntryIterator iterator = becomeMaster.getUnappliedEntries().get()) {
                while (iterator.hasNext()) {
                    Entry entry = iterator.next();
                    if (forMe(entry)) {
                        try {
                            TransactionWithToken transactionWithToken = TransactionWithToken
                                    .parseFrom(entry.getSerializedObject().getData());
                            if( transactionWithToken.getToken() >= nextTransactionToken.get()) {

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
                                if( eventTransactionManager) {
                                    datafileManagerChain.reserveSequenceNumbers(serializedEvents);
                                }
                                nextTransactionToken.addAndGet(transactionWithToken.getEventsCount());
                            }
                        } catch (Exception e) {
                            logger.error("failed: {}", e.getMessage());
                        }
                    }
                }
        }
    }

    private boolean forMe(Entry entry) {
        return entry.hasSerializedObject() && entry.getSerializedObject().getType().equals(entryType);
    }

    public void on(ClusterEvents.LeaderStepDown masterStepDown) {
        nextTransactionToken.set(-1);
        logger.error("{}: Step down", datafileManagerChain.getType());
        datafileManagerChain.stepDown();
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
                RaftNode node = clusterController.getRaftNode(datafileManagerChain.getType().getContext());
                if( node == null || !node.isLeader()) {
                    completableFuture.completeExceptionally(new RuntimeException("No longer leader"));
                    return completableFuture;
                }

                transactionWithToken =
                        TransactionWithToken.newBuilder().setToken(nextTransactionToken.getAndAdd(eventList.size()))
                                            .setVersion(datafileManagerChain.transactionVersion())
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
        return eventList.stream().map(SerializedEvent::asByteString).collect(Collectors.toList());
    }


    @Override
    public long getLastToken() {
        return datafileManagerChain.getLastToken();
    }

    @Override
    public void reserveSequenceNumbers(List<SerializedEvent> eventList) {
        datafileManagerChain.reserveSequenceNumbers(eventList);
    }

    @Override
    public long waitingTransactions() {
        return waitingTransactions.get();
    }

}
