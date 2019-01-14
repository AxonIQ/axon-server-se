package io.axoniq.axonserver.enterprise.storage.transaction;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Marc Gathier
 */
public class ClusterTransactionManager implements StorageTransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(ClusterTransactionManager.class);
    private final EventStore datafileManagerChain;
    private final ReplicationManager replicationManager;

    private final Map<Long, TransactionInformation> activeTransactions = new ConcurrentHashMap<>();
    private final EventTypeContext type;

    public ClusterTransactionManager(
            EventStore datafileManagerChain,
            ReplicationManager replicationManager) {
        this.datafileManagerChain = datafileManagerChain;
        this.replicationManager = replicationManager;
        this.type = datafileManagerChain.getType();
        this.replicationManager.registerListener( type, this::replicationCompleted);
    }

    @Override
    public CompletableFuture<Long> store(List<Event> eventList) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        PreparedTransaction preparedTransaction = datafileManagerChain.prepareTransaction(eventList);
        activeTransactions.put( preparedTransaction.getToken(),
                                new TransactionInformation( completableFuture, replicationManager.getQuorum(type.getContext())));
        try {
            replicationManager.publish(type, eventList, preparedTransaction.getToken());
            datafileManagerChain.store(preparedTransaction).whenComplete((firstToken, cause) -> {
                if( cause == null) {
                    replicationCompleted(firstToken);
                } else {
                    // What if local storage fails
                    logger.error("Failed to store event", cause);
                }
                                                                         });
        } catch( Exception ex) {
            completableFuture.completeExceptionally(ex);
            activeTransactions.remove(preparedTransaction.getToken());
        }
        return completableFuture;
    }

    private void replicationCompleted(long token) {
        TransactionInformation transactionInformation = activeTransactions.get(token);
        if (transactionInformation != null && transactionInformation.completed()) {
            transactionInformation.storageCallback.complete(token);
            activeTransactions.remove(token);
        }
    }

    @Override
    public long waitingTransactions() {
        return activeTransactions.size();
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
    public void rollback(long token) {
        datafileManagerChain.rollback(token);
    }

    @Override
    public void cancelPendingTransactions() {
        activeTransactions.forEach((key, information) -> information.storageCallback.completeExceptionally(new MessagingPlatformException(
                ErrorCode.NO_MASTER_AVAILABLE, "Cancelled as master stepped down during transaction")));
        activeTransactions.clear();
    }

    private class TransactionInformation {

        private final CompletableFuture<Long> storageCallback;
        private final AtomicInteger quorum;

        public TransactionInformation(CompletableFuture<Long> storageCallback, int quorum) {
            this.storageCallback = storageCallback;
            this.quorum = new AtomicInteger(quorum);
        }

        public boolean completed() {
            return quorum.decrementAndGet() == 0;
        }
    }
}
