package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.storage.transaction.RaftTransactionManager;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import org.springframework.context.event.EventListener;

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
    public void on(ClusterEvents.BecomeLeader becomeMaster) {
        if( transactionManagersPerContext.containsKey(becomeMaster.getContext())) {
            transactionManagersPerContext.get(becomeMaster.getContext()).forEach(tm -> tm.on(becomeMaster));
        }
    }

    @EventListener
    public void on(ClusterEvents.LeaderStepDown masterStepDown) {
        if( transactionManagersPerContext.containsKey(masterStepDown.getContextName())) {
            transactionManagersPerContext.get(masterStepDown.getContextName()).forEach(tm -> tm.on(masterStepDown));
        }
    }

}
