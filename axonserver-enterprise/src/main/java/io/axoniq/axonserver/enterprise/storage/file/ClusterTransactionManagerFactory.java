package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.storage.transaction.RaftTransactionManager;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;

/**
 * Author: marc
 */
public class ClusterTransactionManagerFactory implements StorageTransactionManagerFactory {

    private final GrpcRaftController raftController;

    public ClusterTransactionManagerFactory(
            GrpcRaftController raftController) {
        this.raftController = raftController;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        return new RaftTransactionManager(eventStore, raftController);
    }
}
