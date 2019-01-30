package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.EventStore;

/**
 * @author Marc Gathier
 */
public class DefaultStorageTransactionManagerFactory implements StorageTransactionManagerFactory {

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        return new SingleInstanceTransactionManager(eventStore);
    }
}
