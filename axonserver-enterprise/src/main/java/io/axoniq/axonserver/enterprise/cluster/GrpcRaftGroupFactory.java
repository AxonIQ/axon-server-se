package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.cluster.NewConfigurationConsumer;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import io.axoniq.axonserver.enterprise.replication.AxonServerGrpcRaftClientFactory;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftGroup;
import io.axoniq.axonserver.enterprise.replication.SnapshotDataProviders;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Utility component to create a {@link GrpcRaftGroup}.
 *
 * @author Marc Gathier
 * @since 4.3.4
 */
@Component
public class GrpcRaftGroupFactory {

    private final JpaRaftStateRepository raftStateRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final RaftProperties raftProperties;
    private final AxonServerGrpcRaftClientFactory grpcRaftClientFactory;
    private final ApplicationContext applicationContext;
    private final PlatformTransactionManager platformTransactionManager;
    private final ReplicationGroupMemberRepository nodeRepository;
    private final SnapshotDataProviders snapshotDataProviders;
    private final NewConfigurationConsumer newConfigurationConsumer;
    private final ReplicationGroupController replicationGroupController;


    public GrpcRaftGroupFactory(JpaRaftStateRepository raftStateRepository,
                                MessagingPlatformConfiguration messagingPlatformConfiguration,
                                RaftProperties raftProperties,
                                AxonServerGrpcRaftClientFactory grpcRaftClientFactory,
                                ApplicationContext applicationContext,
                                PlatformTransactionManager platformTransactionManager,
                                ReplicationGroupMemberRepository nodeRepository,
                                SnapshotDataProviders snapshotDataProviders,
                                NewConfigurationConsumer newConfigurationConsumer,
                                ReplicationGroupController replicationGroupController) {
        this.raftStateRepository = raftStateRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.raftProperties = raftProperties;
        this.grpcRaftClientFactory = grpcRaftClientFactory;
        this.applicationContext = applicationContext;
        this.platformTransactionManager = platformTransactionManager;
        this.nodeRepository = nodeRepository;
        this.snapshotDataProviders = snapshotDataProviders;
        this.newConfigurationConsumer = newConfigurationConsumer;
        this.replicationGroupController = replicationGroupController;
    }


    /**
     * Creates a {@link RaftGroup} with specified name and local node id.
     *
     * @param groupId     the name of the replication group
     * @param localNodeId the id of the current node in the replication group
     * @return a RaftGroup instance
     */
    public RaftGroup create(String groupId, String localNodeId) {

        RaftGroup raftGroup = new GrpcRaftGroup(localNodeId,
                                                groupId,
                                                raftStateRepository,
                                                nodeRepository,
                                                raftProperties,
                                                snapshotDataProviders,
                                                applicationContext.getBean(LocalEventStore.class),
                                                grpcRaftClientFactory,
                                                messagingPlatformConfiguration,
                                                newConfigurationConsumer,
                                                platformTransactionManager,
                                                replicationGroupController);

        applicationContext.getBeansOfType(LogEntryConsumer.class)
                          .forEach((name, bean) -> raftGroup.localNode()
                                                            .registerEntryConsumer(bean));
        return raftGroup;
    }
}
