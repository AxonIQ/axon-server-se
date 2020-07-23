package io.axoniq.axonserver.enterprise.replication.group;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.replication.RaftLeaderProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import org.springframework.stereotype.Component;

/**
 * Factory to get instances of the {@link RaftGroupService} facade.
 *
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


    /**
     * Returns an instance of {@link RaftGroupService} to send messages to the leader of the group.
     *
     * @param groupId the name of the group
     * @return RaftGroupService to access the leader
     *
     * @throws MessagingPlatformException when no leader found for the group
     */
    public RaftGroupService getRaftGroupService(String groupId) {
        if (raftLeaderProvider.isLeader(groupId)) {
            return localRaftGroupService;
        }
        String leader = raftLeaderProvider.getLeader(groupId);
        if (leader == null) {
            throw new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE,
                                                 "No leader for " + groupId);
        }

        ClusterNode node = clusterController.getNode(leader);
        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(channelProvider.get(node))
                                                              .withInterceptors(new InternalTokenAddingInterceptor(
                                                                      configuration.getAccesscontrol()
                                                                                   .getInternalToken())));
    }

    /**
     * Returns an instance of {@link RaftGroupService} to send messages to a specific node
     *
     * @param nodeName the name of the node
     * @return RaftGroupService to access the node
     */
    public RaftGroupService getRaftGroupServiceForNode(String nodeName) {
        if (configuration.getName().equals(nodeName)) {
            return localRaftGroupService;
        }

        ClusterNode node = clusterController.getNode(nodeName);
        if (node == null) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE, nodeName + " not found");
        }
        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(channelProvider.get(node))
                                                              .withInterceptors(new InternalTokenAddingInterceptor(
                                                                      configuration.getAccesscontrol()
                                                                                   .getInternalToken())));
    }

    /**
     * Returns an instance of {@link RaftGroupService} to send messages to a specific node
     *
     * @param clusterNode definition of the node
     * @return RaftGroupService to access the node
     */
    public RaftGroupService getRaftGroupServiceForNode(ClusterNode clusterNode) {
        if (configuration.getName().equals(clusterNode.getName())) {
            return localRaftGroupService;
        }

        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(channelProvider.get(clusterNode)));
    }

    /**
     * Returns the name of the leader node for a replication group.
     *
     * @param groupId the name of the group
     * @return the name of the leader node
     */
    public String getLeader(String groupId) {
        return raftLeaderProvider.getLeader(groupId);
    }
}
