package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.StorageCallback;

import java.util.List;

/**
 * Author: marc
 */
public interface StorageTransactionManager {

    void store(List<Event> eventList, StorageCallback storageCallback);

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
