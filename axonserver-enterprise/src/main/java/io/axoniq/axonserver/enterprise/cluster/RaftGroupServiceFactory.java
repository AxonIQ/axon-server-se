package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class RaftGroupServiceFactory {

    private final MessagingPlatformConfiguration configuration;
    private final ClusterController clusterController;
    private final RaftGroupService localRaftGroupService;
    private final RaftLeaderProvider raftLeaderProvider;

    public RaftGroupServiceFactory(MessagingPlatformConfiguration configuration,
                                   ClusterController clusterController,
                                   LocalRaftGroupService localRaftGroupService,
                                   RaftLeaderProvider raftLeaderProvider
                              ) {
        this.configuration = configuration;
        this.clusterController = clusterController;
        this.localRaftGroupService = localRaftGroupService;
        this.raftLeaderProvider = raftLeaderProvider;
    }


    public RaftGroupService getRaftGroupService(String groupId) {
        if( raftLeaderProvider.isLeader(groupId)) return localRaftGroupService;

        ClusterNode node = clusterController.getNode(raftLeaderProvider.getLeader(groupId));
        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(ManagedChannelHelper
                .createManagedChannel(configuration, node)));
    }

    public RaftGroupService getRaftGroupServiceForNode(String nodeName) {
        if( configuration.getName().equals(nodeName)) return localRaftGroupService;

        ClusterNode node = clusterController.getNode(nodeName);
        return new RemoteRaftGroupService( RaftGroupServiceGrpc.newStub(ManagedChannelHelper
                                                                                        .createManagedChannel(configuration, node)));
    }

}
