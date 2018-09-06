package io.axoniq.axonhub.localstorage.transaction;

import io.axoniq.axonhub.localstorage.EventStore;

/**
 * Author: marc
 */
public class DefaultStorageTransactionManagerFactory implements StorageTransactionManagerFactory {

    private final boolean clusterEnabled;
    private final ReplicationManager replicationManager;

    public DefaultStorageTransactionManagerFactory() {
        this(false, null);
    }

    public DefaultStorageTransactionManagerFactory(boolean clusterEnabled, ReplicationManager replicationManager) {
        this.clusterEnabled = clusterEnabled;
        this.replicationManager = replicationManager;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        if( ! clusterEnabled || ! eventStore.replicated()) return new SingleInstanceTransactionManager(eventStore);
        return new ClusterTransactionManager(eventStore, replicationManager);
    }
}
