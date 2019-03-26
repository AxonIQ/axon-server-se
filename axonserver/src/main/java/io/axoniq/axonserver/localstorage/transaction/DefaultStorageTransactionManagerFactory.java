package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.EventStorageEngine;

/**
 * @author Marc Gathier
 */
public class DefaultStorageTransactionManagerFactory implements StorageTransactionManagerFactory {

    @Override
    public StorageTransactionManager createTransactionManager(EventStorageEngine eventStore) {
        return new SingleInstanceTransactionManager(eventStore);
    }
}
