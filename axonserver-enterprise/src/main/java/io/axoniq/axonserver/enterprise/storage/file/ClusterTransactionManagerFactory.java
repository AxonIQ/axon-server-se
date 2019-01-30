package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.connector.EventConnector;
import io.axoniq.axonserver.enterprise.cluster.internal.SyncStatusController;
import io.axoniq.axonserver.enterprise.storage.transaction.ClusterTransactionManager;
import io.axoniq.axonserver.enterprise.storage.transaction.ReplicationManager;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class ClusterTransactionManagerFactory implements StorageTransactionManagerFactory {

    private final SyncStatusController syncStatusController;
    private final List<EventConnector> eventConnectors;
    private final ReplicationManager replicationManager;

    public ClusterTransactionManagerFactory(
            SyncStatusController syncStatusController,
            List<EventConnector> eventConnectors,
            ReplicationManager replicationManager) {
        this.syncStatusController = syncStatusController;
        this.eventConnectors = eventConnectors;
        this.replicationManager = replicationManager;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        return new ClusterTransactionManager(eventStore, syncStatusController, eventConnectors, replicationManager);
    }
}
