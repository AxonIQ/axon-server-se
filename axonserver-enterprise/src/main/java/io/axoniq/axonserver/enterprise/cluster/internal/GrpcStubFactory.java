package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.MessagingClusterServiceGrpc;
import io.grpc.ManagedChannel;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Controller;

/**
 * @author Marc Gathier
 */
@Controller("GrpcStubFactory")
public class GrpcStubFactory implements StubFactory {
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;

    public GrpcStubFactory(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public MessagingClusterServiceInterface messagingClusterServiceStub(ClusterNode clusterNode) {
        ManagedChannel managedChannel = ManagedChannelHelper.createManagedChannel(messagingPlatformConfiguration, clusterNode);
        return messagingClusterServiceStub(managedChannel);
    }

    @NotNull
    private MessagingClusterServiceInterface messagingClusterServiceStub(ManagedChannel managedChannel) {
        MessagingClusterServiceGrpc.MessagingClusterServiceStub stub = MessagingClusterServiceGrpc.newStub(managedChannel)
                                                                                                  .withInterceptors(new InternalTokenAddingInterceptor(messagingPlatformConfiguration.getAccesscontrol().getInternalToken()));
        return responseObserver -> stub.openStream(responseObserver);
    }

    @Override
    public MessagingClusterServiceInterface messagingClusterServiceStub( String host, int port) {
        ManagedChannel managedChannel = ManagedChannelHelper.createManagedChannel(messagingPlatformConfiguration, host, port);
        return messagingClusterServiceStub( managedChannel);
    }
}
