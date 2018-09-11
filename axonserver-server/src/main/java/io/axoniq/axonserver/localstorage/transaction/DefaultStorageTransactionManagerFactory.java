package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.enterprise.storage.transaction.ClusterTransactionManager;
import io.axoniq.axonserver.enterprise.storage.transaction.ReplicationManager;
import io.axoniq.axonserver.localstorage.EventStore;

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
