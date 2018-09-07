package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.cluster.jpa.ClusterNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.internal.MessagingClusterServiceInterface;

/**
 * Author: marc
 */
public interface StubFactory {
    MessagingClusterServiceInterface messagingClusterServiceStub(MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode);

    MessagingClusterServiceInterface messagingClusterServiceStub(MessagingPlatformConfiguration messagingPlatformConfiguration, String host, int port);

    DataSychronizationServiceInterface dataSynchronizationServiceStub(MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode);

}
