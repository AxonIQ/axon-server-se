package io.axoniq.axonhub.grpc;

import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.grpc.internal.MessagingClusterServiceInterface;

/**
 * Author: marc
 */
public interface StubFactory {
    MessagingClusterServiceInterface messagingClusterServiceStub(MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode);

    MessagingClusterServiceInterface messagingClusterServiceStub(MessagingPlatformConfiguration messagingPlatformConfiguration, String host, int port);

    DataSychronizationServiceInterface dataSynchronizationServiceStub(MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode);

}
