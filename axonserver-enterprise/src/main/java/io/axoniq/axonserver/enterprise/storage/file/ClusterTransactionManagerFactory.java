package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.enterprise.storage.transaction.ClusterTransactionManager;
import io.axoniq.axonserver.enterprise.storage.transaction.ReplicationManager;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;

/**
 * @author Marc Gathier
 */
public class ClusterTransactionManagerFactory implements StorageTransactionManagerFactory {

    private final ReplicationManager replicationManager;

    public ClusterTransactionManagerFactory(
            ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        return new ClusterTransactionManager(eventStore, replicationManager);
    }
}
