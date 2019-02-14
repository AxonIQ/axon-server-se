package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.grpc.GrpcRaftClientFactory;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.LogReplicationServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import org.springframework.stereotype.Controller;

/**
 * Specific implementation of the GrpcRaftCLientFactory that creates stubs using the ManagedChannelHelper (which is aware of TLS configuration and caches managed channels)
 * It also adds {@link InternalTokenAddingInterceptor} to the stubs to send the internal token in GRPC header with each operation
 * @author Marc Gathier
 */
@Controller
public class AxonServerGrpcRaftClientFactory implements GrpcRaftClientFactory {
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final ClientInterceptor[] interceptors;

    public AxonServerGrpcRaftClientFactory(
            MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.interceptors = new ClientInterceptor[]{
                new InternalTokenAddingInterceptor(messagingPlatformConfiguration.getAccesscontrol().getInternalToken())
        };

    }

    @Override
    public LogReplicationServiceGrpc.LogReplicationServiceStub createLogReplicationServiceStub(Node node) {
        ManagedChannel channel = ManagedChannelHelper.createManagedChannel(messagingPlatformConfiguration, new ClusterNode(node));
        return LogReplicationServiceGrpc.newStub(channel).withInterceptors(interceptors);
    }

    @Override
    public LeaderElectionServiceGrpc.LeaderElectionServiceStub createLeaderElectionStub(Node node) {
        ManagedChannel channel = ManagedChannelHelper.createManagedChannel(messagingPlatformConfiguration, new ClusterNode(node));
        return LeaderElectionServiceGrpc.newStub(channel).withInterceptors(interceptors);
    }
}
