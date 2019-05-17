package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.grpc.internal.MessagingClusterServiceGrpc;
import io.grpc.Channel;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Controller;

/**
 * @author Marc Gathier
 */
@Controller("GrpcStubFactory")
public class GrpcStubFactory implements StubFactory {

    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final ChannelProvider channelProvider;

    public GrpcStubFactory(MessagingPlatformConfiguration messagingPlatformConfiguration,
                           ChannelProvider channelProvider) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.channelProvider = channelProvider;
    }

    @Override
    public MessagingClusterServiceInterface messagingClusterServiceStub(ClusterNode clusterNode) {
        Channel managedChannel = channelProvider.get(clusterNode);
        return messagingClusterServiceStub(managedChannel);
    }

    @NotNull
    private MessagingClusterServiceInterface messagingClusterServiceStub(Channel channel) {
        MessagingClusterServiceGrpc.MessagingClusterServiceStub stub = MessagingClusterServiceGrpc
                .newStub(channel)
                .withInterceptors(new InternalTokenAddingInterceptor(messagingPlatformConfiguration.getAccesscontrol()
                                                                                                   .getInternalToken()));
        return stub::openStream;
    }

    @Override
    public MessagingClusterServiceInterface messagingClusterServiceStub(String host, int port) {
        Channel channel = channelProvider.get(host, port);
        return messagingClusterServiceStub(channel);
    }
}
