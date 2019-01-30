package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;

/**
 * @author Marc Gathier
 */
public interface EventStoreFactory {

    EventStore createEventManagerChain(String context);

    EventStore createSnapshotManagerChain(String context);

    StorageTransactionManager createTransactionManager(EventStore datafileManagerChain);
}
