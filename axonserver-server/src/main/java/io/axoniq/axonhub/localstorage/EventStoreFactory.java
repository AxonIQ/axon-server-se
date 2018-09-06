package io.axoniq.axonhub.localstorage;

import io.axoniq.axonhub.localstorage.transaction.StorageTransactionManager;

/**
 * Author: marc
 */
public interface EventStoreFactory {

    EventStore createEventManagerChain(String context);

    EventStore createSnapshotManagerChain(String context);

    StorageTransactionManager createTransactionManager(EventStore datafileManagerChain);
}
