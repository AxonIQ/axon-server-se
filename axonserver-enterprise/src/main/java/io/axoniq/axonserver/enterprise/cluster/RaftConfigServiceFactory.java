package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import org.springframework.stereotype.Component;

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
        if( raftLeaderProvider.isLeader(GrpcRaftController.ADMIN_GROUP)) return localRaftConfigService;
        String leader = raftLeaderProvider.getLeader(GrpcRaftController.ADMIN_GROUP);
        if( leader == null) throw new RuntimeException("No leader for " + GrpcRaftController.ADMIN_GROUP);
        ClusterNode node = clusterController.getNode(leader);
        return new RemoteRaftConfigService(RaftConfigServiceGrpc.newStub(ManagedChannelHelper
                                                                               .createManagedChannel(configuration, node)));
    }

    public RaftConfigServiceGrpc.RaftConfigServiceBlockingStub getRaftConfigServiceStub(String host, int port) {
        return RaftConfigServiceGrpc.newBlockingStub(ManagedChannelHelper.createManagedChannel(configuration, host, port));
    }

    public RaftConfigService getLocalRaftConfigService() {
        return localRaftConfigService;
    }
}
