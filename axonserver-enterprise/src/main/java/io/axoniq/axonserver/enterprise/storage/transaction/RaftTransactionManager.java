package io.axoniq.axonserver.enterprise.storage.transaction;

import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: marc
 */
public class RaftTransactionManager implements StorageTransactionManager {
    private final EventStore datafileManagerChain;
    private final GrpcRaftController clusterController;
    private final AtomicLong token = new AtomicLong();


    public RaftTransactionManager(EventStore datafileManagerChain,
                                  GrpcRaftController clusterController) {
        this.datafileManagerChain = datafileManagerChain;
        this.clusterController = clusterController;
    }

    @Override
    public CompletableFuture<Long> store(List<Event> eventList) {
        token.compareAndSet(0, datafileManagerChain.getLastToken()+1);
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        RaftNode node = clusterController.getStorageRaftNode(datafileManagerChain.getType().getContext());

        // The following 2 lines must be run by one thread at a time
        TransactionWithToken transactionWithToken = TransactionWithToken.newBuilder().setToken(token.getAndAdd(eventList.size())).addAllEvents(eventList).build();
        CompletableFuture<Void> appendEntryResult = node.appendEntry(
                "Append." + datafileManagerChain.getType().getEventType(), transactionWithToken.toByteArray());
        //
        appendEntryResult.whenComplete((r, cause) -> {
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
        return 0;
    }

    @Override
    public void rollback(long token) {

    }

    @Override
    public void cancelPendingTransactions() {

    }
}
