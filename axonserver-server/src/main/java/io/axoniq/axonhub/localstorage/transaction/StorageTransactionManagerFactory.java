package io.axoniq.axonhub.localstorage.transaction;

import io.axoniq.axonhub.localstorage.EventStore;

/**
 * Author: marc
 */
public interface StorageTransactionManagerFactory {
    StorageTransactionManager createTransactionManager(EventStore eventStore);
}
