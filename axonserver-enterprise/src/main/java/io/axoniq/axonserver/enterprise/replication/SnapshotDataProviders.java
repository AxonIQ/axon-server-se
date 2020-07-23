package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.access.application.AdminApplicationController;
import io.axoniq.axonserver.access.application.ReplicationGroupApplicationController;
import io.axoniq.axonserver.access.user.ReplicationGroupUserController;
import io.axoniq.axonserver.access.user.UserRepository;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.replication.snapshot.AdminContextSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.AdminApplicationSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.AdminClusterSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.ApplicationSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.ContextSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.LegacyReplicationGroupSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.UserSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.EventTransactionsSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.AdminProcessorLoadBalancingSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.ProcessorLoadBalancingSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.AdminReplicationGroupSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.SnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.SnapshotTransactionsSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.TaskSnapshotDataStore;
import io.axoniq.axonserver.enterprise.replication.snapshot.AdminUserSnapshotDataStore;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.AdminProcessorLoadBalancingRepository;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ReplicationGroupProcessorLoadBalancingRepository;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.enterprise.storage.multitier.LowerTierEventStoreLocator;
import io.axoniq.axonserver.enterprise.taskscheduler.ClusterTaskManager;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * Providers of the list of {@link SnapshotDataStore}s needed to replicate the full state.
 * @author Sara Pellegrini
 * @since 4.1
 */
@Component
public class SnapshotDataProviders implements Function<String, List<SnapshotDataStore>> {

    private final AdminApplicationController applicationController;

    private final UserRepository userRepository;

    private final ReplicationGroupUserController contextUserRepository;
    private final AdminProcessorLoadBalancingRepository processorLoadBalancingRepository;
    private final ReplicationGroupProcessorLoadBalancingRepository raftProcessorLoadBalancingRepository;

    private final ReplicationGroupApplicationController contextApplicationController;
    private final AdminReplicationGroupController adminReplicationGroupController;
    private final ReplicationGroupController replicationGroupController;
    private final ClusterController clusterController;
    private final ApplicationContext applicationContext;

    public SnapshotDataProviders(
            AdminApplicationController applicationController,
            UserRepository userRepository,
            ReplicationGroupUserController contextUserRepository,
            AdminProcessorLoadBalancingRepository processorLoadBalancingRepository,
            ReplicationGroupProcessorLoadBalancingRepository raftProcessorLoadBalancingRepository,
            ReplicationGroupApplicationController contextApplicationController,
            AdminReplicationGroupController adminReplicationGroupController,
            ReplicationGroupController replicationGroupController,
            ClusterController clusterController,
            ApplicationContext applicationContext) {
        this.applicationController = applicationController;
        this.userRepository = userRepository;
        this.contextUserRepository = contextUserRepository;
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
        this.raftProcessorLoadBalancingRepository = raftProcessorLoadBalancingRepository;
        this.contextApplicationController = contextApplicationController;
        this.adminReplicationGroupController = adminReplicationGroupController;
        this.replicationGroupController = replicationGroupController;
        this.clusterController = clusterController;
        this.applicationContext = applicationContext;
    }

    /**
     * Returns the list of {@link SnapshotDataStore}s needed to replicate the full state for the specified context.
     * @param context the context
     * @return the list of {@link SnapshotDataStore}s
     */
    public List<SnapshotDataStore> apply(String context){
        LocalEventStore localEventStore = applicationContext.getBean(LocalEventStore.class);
        LowerTierEventStoreLocator lowerTierEventStoreLocator = applicationContext
                .getBean(LowerTierEventStoreLocator.class);
        return asList(
                new AdminClusterSnapshotDataStore(context, clusterController),
                new AdminReplicationGroupSnapshotDataStore(context, adminReplicationGroupController),
                new LegacyReplicationGroupSnapshotDataStore(context, adminReplicationGroupController),
                new ContextSnapshotDataStore(context, replicationGroupController, localEventStore),
                new AdminContextSnapshotDataStore(context, adminReplicationGroupController),
                new AdminApplicationSnapshotDataStore(context, applicationController),
                new ApplicationSnapshotDataStore(context, replicationGroupController, contextApplicationController),
                new EventTransactionsSnapshotDataStore(context,
                                                       localEventStore,
                                                       lowerTierEventStoreLocator,
                                                       replicationGroupController),
                new SnapshotTransactionsSnapshotDataStore(context, localEventStore,
                                                          lowerTierEventStoreLocator,
                                                          replicationGroupController),
                new ProcessorLoadBalancingSnapshotDataStore(context,
                                                            replicationGroupController,
                                                            raftProcessorLoadBalancingRepository),
                new AdminProcessorLoadBalancingSnapshotDataStore(context, processorLoadBalancingRepository),
                new AdminUserSnapshotDataStore(context, userRepository),
                new UserSnapshotDataStore(context, replicationGroupController, contextUserRepository),
                new TaskSnapshotDataStore(context,
                                          applicationContext.getBean(ClusterTaskManager.class))
        );
    }

}
