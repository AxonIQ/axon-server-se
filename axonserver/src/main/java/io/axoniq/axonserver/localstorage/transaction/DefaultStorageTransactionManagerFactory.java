package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.EventStore;

/**
 * Author: marc
 */
public class DefaultStorageTransactionManagerFactory implements StorageTransactionManagerFactory {

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        return new SingleInstanceTransactionManager(eventStore);
    }
}
