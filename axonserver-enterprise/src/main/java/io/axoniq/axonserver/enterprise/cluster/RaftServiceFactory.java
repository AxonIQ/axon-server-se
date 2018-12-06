package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class RaftServiceFactory {

    private final MessagingPlatformConfiguration configuration;
    private final ClusterController clusterController;

    public RaftServiceFactory(MessagingPlatformConfiguration configuration,
                              ClusterController clusterController) {
        this.configuration = configuration;
        this.clusterController = clusterController;
    }

    public RaftLeaderService getRaftService(RaftGroup raftGroup) {
        if( raftGroup.localNode().isLeader()) return new LocalRaftLeaderService(raftGroup.localNode().groupId(), raftGroup.localNode());

        ClusterNode node = clusterController.getNode(raftGroup.localNode().getLeader());
        return new RemoteRaftLeaderService(raftGroup.localNode().groupId(), RaftGroupServiceGrpc.newStub(ManagedChannelHelper
                .createManagedChannel(configuration, node)));
    }

    public RaftConfigServiceGrpc.RaftConfigServiceBlockingStub getRaftConfigService(String host, int port) {
        return RaftConfigServiceGrpc.newBlockingStub(ManagedChannelHelper.createManagedChannel(configuration, host, port));
    }
}
