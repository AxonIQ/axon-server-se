package io.axoniq.axonserver.enterprise.storage.transaction;

import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Author: marc
 */
public class RaftTransactionManager implements StorageTransactionManager {
    private final EventStore datafileManagerChain;
    private final GrpcRaftController clusterController;
    private final AtomicLong token = new AtomicLong();
    private final AtomicBoolean allocating = new AtomicBoolean();
    private final AtomicLong waitingTransactions = new AtomicLong();


    public RaftTransactionManager(EventStore datafileManagerChain,
                                  GrpcRaftController clusterController) {
        this.datafileManagerChain = datafileManagerChain;
        this.clusterController = clusterController;
    }

    @Override
    public CompletableFuture<Long> store(List<Event> eventList) {
        waitingTransactions.incrementAndGet();
        token.compareAndSet(0, datafileManagerChain.getLastToken()+1);
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        RaftNode node = clusterController.getStorageRaftNode(datafileManagerChain.getType().getContext());
        // Ensure that only one thread is generating transaction token and log entry index at the same time
        while( !allocating.compareAndSet(false, true)) {
            LockSupport.parkNanos(1000);
        }
        CompletableFuture<Void> appendEntryResult;
        TransactionWithToken transactionWithToken;
        try {
            transactionWithToken = TransactionWithToken.newBuilder().setToken(token.getAndAdd(
                    eventList.size())).addAllEvents(eventList).build();
            appendEntryResult = node.appendEntry(
                    "Append." + datafileManagerChain.getType().getEventType(), transactionWithToken.toByteArray());
        } finally {
            allocating.set(false);
        }
        //
        appendEntryResult.whenComplete((r, cause) -> {
            waitingTransactions.decrementAndGet();
            if( cause == null) {
                completableFuture.complete(transactionWithToken.getToken());
            } else {
                completableFuture.completeExceptionally(cause);
            }
        });
        return completableFuture;
    }


    @Override
    public long getLastToken() {
        return datafileManagerChain.getLastToken();
    }

    @Override
    public void reserveSequenceNumbers(List<Event> eventList) {
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
}
