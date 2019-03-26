package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Defines the interface for a transaction manager.
 * @author Marc Gathier
 */
public interface StorageTransactionManager {

    CompletableFuture<Long> store(List<SerializedEvent> eventList);

    void reserveSequenceNumbers(List<SerializedEvent> eventList);

    default long waitingTransactions() {
        return 0;
    }

    default void cancelPendingTransactions() {

    }
}
