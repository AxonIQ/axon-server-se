package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.cluster.NewConfigurationConsumer;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * @author Marc Gathier
 */
@Component
public class GrpcRaftGroupFactory {

    private final JpaRaftStateRepository raftStateRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final RaftProperties raftProperties;
    private final AxonServerGrpcRaftClientFactory grpcRaftClientFactory;
    private final ApplicationContext applicationContext;
    private final PlatformTransactionManager platformTransactionManager;
    private final JpaRaftGroupNodeRepository nodeRepository;
    private final SnapshotDataProviders snapshotDataProviders;
    private final NewConfigurationConsumer newConfigurationConsumer;


    public GrpcRaftGroupFactory(JpaRaftStateRepository raftStateRepository,
                                MessagingPlatformConfiguration messagingPlatformConfiguration,
                                RaftProperties raftProperties,
                                AxonServerGrpcRaftClientFactory grpcRaftClientFactory,
                                ApplicationContext applicationContext,
                                PlatformTransactionManager platformTransactionManager,
                                JpaRaftGroupNodeRepository nodeRepository,
                                SnapshotDataProviders snapshotDataProviders,
                                NewConfigurationConsumer newConfigurationConsumer) {
        this.raftStateRepository = raftStateRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.raftProperties = raftProperties;
        this.grpcRaftClientFactory = grpcRaftClientFactory;
        this.applicationContext = applicationContext;
        this.platformTransactionManager = platformTransactionManager;
        this.nodeRepository = nodeRepository;
        this.snapshotDataProviders = snapshotDataProviders;
        this.newConfigurationConsumer = newConfigurationConsumer;
    }


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
                                                platformTransactionManager);

        applicationContext.getBeansOfType(LogEntryConsumer.class)
                          .forEach((name, bean) -> raftGroup.localNode()
                                                            .registerEntryConsumer(bean));
        return raftGroup;
    }
}
