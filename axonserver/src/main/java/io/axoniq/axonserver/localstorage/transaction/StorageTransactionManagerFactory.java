package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.EventStore;

/**
 * @author Marc Gathier
 */
public interface StorageTransactionManagerFactory {
    StorageTransactionManager createTransactionManager(EventStore eventStore);
}
