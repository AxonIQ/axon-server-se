package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.jpa.ClusterNode;

/**
 * @author Marc Gathier
 */
public interface StubFactory {
    default MessagingClusterServiceInterface messagingClusterServiceStub(ClusterNode clusterNode) {
        throw new UnsupportedOperationException();
    }

    default MessagingClusterServiceInterface messagingClusterServiceStub(String host, int port) {
        throw new UnsupportedOperationException();
    }

}
