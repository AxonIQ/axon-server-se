package io.axoniq.axonserver.enterprise.replication.admin;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.replication.RaftLeaderProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Factory to get a {@link RaftConfigService}.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class RaftConfigServiceFactory {

    private final MessagingPlatformConfiguration configuration;
    private final ClusterController clusterController;
    private final RaftConfigService localRaftConfigService;
    private final RaftLeaderProvider raftLeaderProvider;
    private final ChannelProvider channelProvider;

    public RaftConfigServiceFactory(MessagingPlatformConfiguration configuration,
                                    ClusterController clusterController,
                                    LocalRaftConfigService localRaftConfigService,
                                    RaftLeaderProvider raftLeaderProvider,
                                    ChannelProvider channelProvider) {
        this.configuration = configuration;
        this.clusterController = clusterController;
        this.localRaftConfigService = localRaftConfigService;
        this.raftLeaderProvider = raftLeaderProvider;
        this.channelProvider = channelProvider;
    }


    /**
     * If the current node is the leader for the replication group returns the {@link LocalRaftConfigService},
     * otherwise it creates a new {@link RemoteRaftConfigService} instance to the leader.
     *
     * @return facade to raft config service
     *
     * @throws MessagingPlatformException if no leader is found for the replication group
     */
    public RaftConfigService getRaftConfigService() {
        if (raftLeaderProvider.isLeader(getAdmin())) {
            return localRaftConfigService;
        }
        String leader = raftLeaderProvider.getLeader(getAdmin());
        if (leader == null) {
            throw new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE,
                                                 "No leader for " + getAdmin());
        }
        ClusterNode node = clusterController.getNode(leader);
        if (node == null) {
            throw new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE,
                                                 "Leader node " + leader + " not found for " + getAdmin());
        }
        return new RemoteRaftConfigService(RaftConfigServiceGrpc.newStub(channelProvider.get(node))
                                                                .withInterceptors(new InternalTokenAddingInterceptor(
                                                                        configuration.getAccesscontrol()
                                                                                     .getInternalToken())));
    }

    /**
     * Returns a {@link RaftConfigServiceGrpc} stub for a remote node, based on its hostname and port.
     *
     * @param host the host to connect to
     * @param port the port to use
     * @return a stub
     */
    public RaftConfigServiceGrpc.RaftConfigServiceBlockingStub getRaftConfigServiceStub(String host, int port) {
        return RaftConfigServiceGrpc.newBlockingStub(channelProvider.get(host, port))
                                    .withInterceptors(new InternalTokenAddingInterceptor(configuration
                                                                                                 .getAccesscontrol()
                                                                                                 .getInternalToken()));
    }

    /**
     * Retrieves the local {@link RaftConfigService} facade.
     *
     * @return the local {@link RaftConfigService}
     */
    public RaftConfigService getLocalRaftConfigService() {
        return localRaftConfigService;
    }
}
