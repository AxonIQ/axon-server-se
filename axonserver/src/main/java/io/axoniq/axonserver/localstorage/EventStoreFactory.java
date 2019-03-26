package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;

/**
 * Defines the interface to configure an event store for a specific context.
 * @author Marc Gathier
 */
public interface EventStoreFactory {

    /**
     * Creates the storage engine for events for a context.
     * @param context the context
     * @return the storage engine
     */
    EventStorageEngine createEventStorageEngine(String context);

    /**
     * Creates the storage engine for snapshots for a context.
     * @param context the context
     * @return the storage engine
     */
    EventStorageEngine createSnapshotStorageEngine(String context);

    /**
     * Creates a transaction manager for a specific storage engine.
     * @param eventStorageEngine the storage engine
     * @return the transaction manager
     */
    StorageTransactionManager createTransactionManager(EventStorageEngine eventStorageEngine);
}
