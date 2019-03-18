package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
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
        String leader = raftLeaderProvider.getLeader(groupId);
        if( leader == null) throw new RuntimeException("No leader for " + groupId);

        ClusterNode node = clusterController.getNode(leader);
        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(ManagedChannelHelper.createManagedChannel(configuration, node))
                                                              .withInterceptors(new InternalTokenAddingInterceptor(configuration.getAccesscontrol().getInternalToken())));
    }

    public RaftGroupService getRaftGroupServiceForNode(String nodeName) {
        if( configuration.getName().equals(nodeName)) return localRaftGroupService;

        ClusterNode node = clusterController.getNode(nodeName);
        return new RemoteRaftGroupService( RaftGroupServiceGrpc.newStub(ManagedChannelHelper
                                                                                        .createManagedChannel(configuration, node))
                                                              .withInterceptors(new InternalTokenAddingInterceptor(configuration.getAccesscontrol().getInternalToken())));
    }

    public RaftGroupService getRaftGroupServiceForNode(ClusterNode clusterNode) {
        if( configuration.getName().equals(clusterNode.getName())) return localRaftGroupService;

        return new RemoteRaftGroupService( RaftGroupServiceGrpc.newStub(ManagedChannelHelper
                                                                                .createManagedChannel(configuration, clusterNode)));
    }

    public String getLeader(String context) {
        return raftLeaderProvider.getLeader(context);
    }
}
