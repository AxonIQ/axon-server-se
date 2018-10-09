package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public interface StorageTransactionManager {

    CompletableFuture<Long> store(List<Event> eventList);

    long getLastToken();

    void reserveSequenceNumbers(List<Event> eventList);

    default long waitingTransactions() {
        return 0;
    }

    default void rollback(long token) {
        // default no-op
    }

    default void cancelPendingTransactions() {

    }
}
