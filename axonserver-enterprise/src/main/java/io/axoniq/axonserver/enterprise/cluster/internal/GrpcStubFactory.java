package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.MessagingClusterServiceGrpc;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Controller;

/**
 * Author: marc
 */
@Controller("GrpcStubFactory")
public class GrpcStubFactory implements StubFactory {
    @Override
    public MessagingClusterServiceInterface messagingClusterServiceStub(MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode) {
        ManagedChannel managedChannel = ManagedChannelHelper.createManagedChannel(messagingPlatformConfiguration, clusterNode);
        return messagingClusterServiceStub(messagingPlatformConfiguration, managedChannel);
    }

    @NotNull
    private MessagingClusterServiceInterface messagingClusterServiceStub(
            MessagingPlatformConfiguration messagingPlatformConfiguration, ManagedChannel managedChannel) {
        MessagingClusterServiceGrpc.MessagingClusterServiceStub stub = MessagingClusterServiceGrpc.newStub(managedChannel)
                                                                                                  .withInterceptors(new InternalTokenAddingInterceptor(messagingPlatformConfiguration.getAccesscontrol().getInternalToken()));
        return new MessagingClusterServiceInterface() {
            @Override
            public StreamObserver<ConnectorCommand> openStream(StreamObserver<ConnectorResponse> responseObserver) {
                return stub.openStream(responseObserver);
            }

            @Override
            public void join(NodeInfo request, StreamObserver<NodeInfo> responseObserver) {
                stub.join(request, responseObserver);
            }

        };
    }

    @Override
    public MessagingClusterServiceInterface messagingClusterServiceStub(
            MessagingPlatformConfiguration messagingPlatformConfiguration, String host, int port) {
        ManagedChannel managedChannel = ManagedChannelHelper.createManagedChannel(messagingPlatformConfiguration, host, port);
        return messagingClusterServiceStub(messagingPlatformConfiguration, managedChannel);
    }
}
