package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class RaftGroupServiceFactory {

    private final MessagingPlatformConfiguration configuration;
    private final ClusterController clusterController;
    private final RaftGroupService localRaftGroupService;
    private final RaftLeaderProvider raftLeaderProvider;
    private final ChannelProvider channelProvider;

    public RaftGroupServiceFactory(MessagingPlatformConfiguration configuration,
                                   ClusterController clusterController,
                                   LocalRaftGroupService localRaftGroupService,
                                   RaftLeaderProvider raftLeaderProvider,
                                   ChannelProvider channelProvider) {
        this.configuration = configuration;
        this.clusterController = clusterController;
        this.localRaftGroupService = localRaftGroupService;
        this.raftLeaderProvider = raftLeaderProvider;
        this.channelProvider = channelProvider;
    }


    public RaftGroupService getRaftGroupService(String groupId) {
        if( raftLeaderProvider.isLeader(groupId)) return localRaftGroupService;
        String leader = raftLeaderProvider.getLeader(groupId);
        if( leader == null) throw new RuntimeException("No leader for " + groupId);

        ClusterNode node = clusterController.getNode(leader);
        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(channelProvider.get(node))
                                                              .withInterceptors(new InternalTokenAddingInterceptor(configuration.getAccesscontrol().getInternalToken())));
    }

    public RaftGroupService getRaftGroupServiceForNode(String nodeName) {
        if( configuration.getName().equals(nodeName)) return localRaftGroupService;

        ClusterNode node = clusterController.getNode(nodeName);
        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(channelProvider.get(node))
                                                              .withInterceptors(new InternalTokenAddingInterceptor(configuration.getAccesscontrol().getInternalToken())));
    }

    public RaftGroupService getRaftGroupServiceForNode(ClusterNode clusterNode) {
        if( configuration.getName().equals(clusterNode.getName())) return localRaftGroupService;

        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(channelProvider.get(clusterNode)));
    }

    public String getLeader(String context) {
        return raftLeaderProvider.getLeader(context);
    }
}
