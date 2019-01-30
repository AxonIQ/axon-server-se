package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Marc Gathier
 */
public interface StorageTransactionManager {

    CompletableFuture<Long> store(List<SerializedEvent> eventList);

    long getLastToken();

    void reserveSequenceNumbers(List<SerializedEvent> eventList);

    default long waitingTransactions() {
        return 0;
    }

    default void rollback(long token) {
        // default no-op
    }

    default void cancelPendingTransactions() {

    }

    long getLastIndex();
}
