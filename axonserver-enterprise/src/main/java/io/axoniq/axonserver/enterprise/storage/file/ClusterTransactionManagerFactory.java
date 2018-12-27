package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.enterprise.cluster.internal.SyncStatusController;
import io.axoniq.axonserver.enterprise.storage.transaction.ClusterTransactionManager;
import io.axoniq.axonserver.enterprise.storage.transaction.ReplicationManager;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;

/**
 * Author: marc
 */
public class ClusterTransactionManagerFactory implements StorageTransactionManagerFactory {

    private final SyncStatusController syncStatusController;
    private final ReplicationManager replicationManager;

    public ClusterTransactionManagerFactory(
            SyncStatusController syncStatusController,
            ReplicationManager replicationManager) {
        this.syncStatusController = syncStatusController;
        this.replicationManager = replicationManager;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        return new ClusterTransactionManager(eventStore, syncStatusController, replicationManager);
    }
}
