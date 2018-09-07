package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.EventStore;

/**
 * Author: marc
 */
public interface StorageTransactionManagerFactory {
    StorageTransactionManager createTransactionManager(EventStore eventStore);
}
