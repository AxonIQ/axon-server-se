package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Author: marc
 */
@Component
public class RaftConfigServiceFactory {

    private final MessagingPlatformConfiguration configuration;
    private final ClusterController clusterController;
    private final RaftConfigService localRaftConfigService;
    private final RaftLeaderProvider raftLeaderProvider;

    public RaftConfigServiceFactory(MessagingPlatformConfiguration configuration,
                                    ClusterController clusterController,
                                    LocalRaftConfigService localRaftConfigService,
                                    RaftLeaderProvider raftLeaderProvider
                              ) {
        this.configuration = configuration;
        this.clusterController = clusterController;
        this.localRaftConfigService = localRaftConfigService;
        this.raftLeaderProvider = raftLeaderProvider;
    }


    public RaftConfigService getRaftConfigService() {
        if( raftLeaderProvider.isLeader(getAdmin()) ) return localRaftConfigService;
        String leader = raftLeaderProvider.getLeader(getAdmin());
        if( leader == null) throw new RuntimeException("No leader for " + getAdmin());
        ClusterNode node = clusterController.getNode(leader);
        return new RemoteRaftConfigService(RaftConfigServiceGrpc.newStub(ManagedChannelHelper
                                                                               .createManagedChannel(configuration, node))
                                                                .withInterceptors(new InternalTokenAddingInterceptor(configuration.getAccesscontrol().getInternalToken())));
    }

    public RaftConfigServiceGrpc.RaftConfigServiceBlockingStub getRaftConfigServiceStub(String host, int port) {
        return RaftConfigServiceGrpc.newBlockingStub(ManagedChannelHelper.createManagedChannel(configuration, host, port))
                                    .withInterceptors(new InternalTokenAddingInterceptor(configuration.getAccesscontrol().getInternalToken()));
    }

    public RaftConfigService getLocalRaftConfigService() {
        return localRaftConfigService;
    }
}
