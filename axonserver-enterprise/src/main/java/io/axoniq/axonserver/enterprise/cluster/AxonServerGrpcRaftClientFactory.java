package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.grpc.GrpcRaftClientFactory;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.LogReplicationServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import org.springframework.stereotype.Controller;

/**
 * Specific implementation of the GrpcRaftCLientFactory that creates stubs using the ManagedChannelHelper (which is aware of TLS configuration and caches managed channels)
 * It also adds {@link InternalTokenAddingInterceptor} to the stubs to send the internal token in GRPC header with each operation
 * @author Marc Gathier
 */
@Controller
public class AxonServerGrpcRaftClientFactory implements GrpcRaftClientFactory {
    private final ClientInterceptor[] interceptors;
    private final ChannelProvider channelProvider;

    public AxonServerGrpcRaftClientFactory(
            MessagingPlatformConfiguration messagingPlatformConfiguration,
            ChannelProvider channelProvider) {
        this.interceptors = new ClientInterceptor[]{
                new InternalTokenAddingInterceptor(messagingPlatformConfiguration.getAccesscontrol().getInternalToken())
        };

        this.channelProvider = channelProvider;
    }

    @Override
    public LogReplicationServiceGrpc.LogReplicationServiceStub createLogReplicationServiceStub(Node node) {
        Channel channel = channelProvider.get(node);
        return LogReplicationServiceGrpc.newStub(channel).withInterceptors(interceptors);
    }

    @Override
    public LeaderElectionServiceGrpc.LeaderElectionServiceStub createLeaderElectionStub(Node node) {
        Channel channel = channelProvider.get(node);
        return LeaderElectionServiceGrpc.newStub(channel).withInterceptors(interceptors);
    }
}
