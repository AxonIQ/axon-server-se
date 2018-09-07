package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;

/**
 * Author: marc
 */
public interface EventStoreFactory {

    EventStore createEventManagerChain(String context);

    EventStore createSnapshotManagerChain(String context);

    StorageTransactionManager createTransactionManager(EventStore datafileManagerChain);
}
