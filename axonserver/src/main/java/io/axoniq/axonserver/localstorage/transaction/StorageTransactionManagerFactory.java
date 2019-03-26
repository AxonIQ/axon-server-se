package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.EventStorageEngine;

/**
 * Defines the factory to create a transaction manager.
 * @author Marc Gathier
 */
public interface StorageTransactionManagerFactory {
    StorageTransactionManager createTransactionManager(EventStorageEngine eventStore);
}
