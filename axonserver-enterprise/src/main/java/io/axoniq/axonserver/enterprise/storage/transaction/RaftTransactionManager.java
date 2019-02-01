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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class RaftTransactionManager implements StorageTransactionManager {
    private final Logger logger = LoggerFactory.getLogger(RaftTransactionManager.class);
    private final EventStore datafileManagerChain;
    private final GrpcRaftController clusterController;
    private final AtomicLong token = new AtomicLong();
    private final AtomicBoolean allocating = new AtomicBoolean();
    private final AtomicInteger waitingTransactions = new AtomicInteger();


    public RaftTransactionManager(EventStore datafileManagerChain,
                                  GrpcRaftController clusterController) {
        this.datafileManagerChain = datafileManagerChain;
        this.clusterController = clusterController;
    }

    public void on(ClusterEvents.BecomeLeader becomeMaster) {
        waitingTransactions.set(0);
        token.set(datafileManagerChain.lastIndex());
        if(EventType.EVENT.equals(datafileManagerChain.getType().getEventType())) {
            try (EntryIterator iterator = becomeMaster.getUnappliedEntries().get()) {
                while (iterator.hasNext()) {
                    Entry entry = iterator.next();
                    if (forMe(entry)) {
                        try {
                            TransactionWithToken transactionWithToken = TransactionWithToken
                                    .parseFrom(entry.getSerializedObject().getData());

                            List<SerializedEvent> serializedEvents = transactionWithToken.getEventsList()
                                    .stream()
                                    .map(bytes -> new SerializedEvent(bytes.toByteArray()))
                                    .collect(Collectors.toList());
                            if( logger.isInfoEnabled()) {
                                serializedEvents.forEach(e -> logger.info("{}/{} reserve sequence numbers for {} : {}",
                                                                      entry.getTerm(),
                                                                      entry.getIndex(),
                                                                      e.getAggregateIdentifier(),
                                                                      e.getAggregateSequenceNumber()));
                            }
                            datafileManagerChain.reserveSequenceNumbers(serializedEvents);
                            token.updateAndGet(old -> Math.max(old, transactionWithToken.getIndex()));
                        } catch (Exception e) {
                            logger.error("failed: {}", e.getMessage());
                        }
                    }
                }
            }
        }
        token.incrementAndGet();
    }

    private boolean forMe(Entry entry) {
        return entry.hasSerializedObject() && entry.getSerializedObject().getType().equals("Append." + datafileManagerChain.getType().getEventType());
    }

    public void on(ClusterEvents.LeaderStepDown masterStepDown) {
        token.set(-1);
        logger.error("{}: Step down", datafileManagerChain.getType());
        datafileManagerChain.stepDown();
    }

    @Override
    public CompletableFuture<Long> store(List<SerializedEvent> eventList) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            // Ensure that only one thread is generating transaction token and log entry index at the same time
            long before = System.nanoTime();
            while (!allocating.compareAndSet(false, true)) {
            }
            long after = System.nanoTime();
            if( after - before > 10000) {
                logger.debug("Waited {} nanos for access", (after - before));
            }
            CompletableFuture<Void> appendEntryResult;
            TransactionWithToken transactionWithToken;
            try {
                RaftNode node = clusterController.getRaftNode(datafileManagerChain.getType().getContext());
                if( node == null || !node.isLeader()) {
                    completableFuture.completeExceptionally(new RuntimeException("No longer leader"));
                    return completableFuture;
                }

                transactionWithToken =
                        TransactionWithToken.newBuilder().setIndex(token.getAndIncrement())
                                            .setVersion(datafileManagerChain.transactionVersion())
                .addAllEvents(eventsAsByteStrings(eventList)).build();

                if( logger.isTraceEnabled()) {
                    logger.trace("Append transaction: {} with {} events",
                                 transactionWithToken.getIndex(),
                                 transactionWithToken.getEventsCount());
                }
                appendEntryResult = node.appendEntry(
                        "Append." + datafileManagerChain.getType().getEventType(), transactionWithToken.toByteArray());
                waitingTransactions.incrementAndGet();
            } finally {
                allocating.set(false);
            }
            //
            appendEntryResult.whenComplete((r, cause) -> {
                waitingTransactions.decrementAndGet();
                if (cause == null) {
                    completableFuture.complete(transactionWithToken.getIndex());
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

    @Override
    public void rollback(long token) {

    }

    @Override
    public void cancelPendingTransactions() {

    }

    @Override
    public long getLastIndex() {
        return datafileManagerChain.lastIndex();
    }
}
