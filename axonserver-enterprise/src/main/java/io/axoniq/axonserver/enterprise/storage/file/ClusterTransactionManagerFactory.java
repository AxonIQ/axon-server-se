package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.storage.transaction.RaftTransactionManager;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Marc Gathier
 */
public class ClusterTransactionManagerFactory implements StorageTransactionManagerFactory {

    private final GrpcRaftController raftController;
    private final MessagingPlatformConfiguration configuration;
    private final Map<String, Set<RaftTransactionManager>> transactionManagersPerContext = new ConcurrentHashMap<>();

    public ClusterTransactionManagerFactory(
            GrpcRaftController raftController, MessagingPlatformConfiguration configuration) {
        this.raftController = raftController;
        this.configuration = configuration;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStorageEngine eventStore) {
        RaftTransactionManager raftTransactionManager = new RaftTransactionManager(eventStore, raftController, configuration);
        transactionManagersPerContext.computeIfAbsent(eventStore.getType().getContext(),
                                                      k -> new CopyOnWriteArraySet<>()).add(raftTransactionManager);
        return raftTransactionManager;
    }

    @EventListener
    @Order(1)
    public void on(ClusterEvents.BecomeContextLeader becomeMaster) {
        if (transactionManagersPerContext.containsKey(becomeMaster.context())) {
            transactionManagersPerContext.get(becomeMaster.context()).forEach(tm -> tm.on(becomeMaster));
        }
    }

    @EventListener
    public void on(ClusterEvents.ContextLeaderStepDown masterStepDown) {
        if (transactionManagersPerContext.containsKey(masterStepDown.context())) {
            transactionManagersPerContext.get(masterStepDown.context()).forEach(tm -> tm.on(masterStepDown));
        }
    }

}
