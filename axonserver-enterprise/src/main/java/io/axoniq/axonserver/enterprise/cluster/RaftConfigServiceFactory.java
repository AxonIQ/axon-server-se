package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
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


    public RaftConfigService getRaftConfigService() {
        if( raftLeaderProvider.isLeader(getAdmin()) ) return localRaftConfigService;
        String leader = raftLeaderProvider.getLeader(getAdmin());
        if (leader == null) {
            throw new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE,
                                                 "No leader for " + getAdmin());
        }
        ClusterNode node = clusterController.getNode(leader);
        return new RemoteRaftConfigService(RaftConfigServiceGrpc.newStub(channelProvider.get(node))
                                                                .withInterceptors(new InternalTokenAddingInterceptor(configuration.getAccesscontrol().getInternalToken())));
    }

    public RaftConfigServiceGrpc.RaftConfigServiceBlockingStub getRaftConfigServiceStub(String host, int port) {
        return RaftConfigServiceGrpc.newBlockingStub(channelProvider.get(host, port))
                                    .withInterceptors(new InternalTokenAddingInterceptor(configuration.getAccesscontrol().getInternalToken()));
    }

    public RaftConfigService getLocalRaftConfigService() {
        return localRaftConfigService;
    }
}
