package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.Node;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
public interface ChannelCloser {

    boolean shutdownIfNeeded(String hostname, int port, Throwable throwable);

    default boolean shutdownIfNeeded(ClusterNode node, Throwable throwable) {
        return shutdownIfNeeded(node.getInternalHostName(), node.getGrpcInternalPort(), throwable);
    }

    default boolean shutdownIfNeeded(Node node, Throwable throwable) {
        return shutdownIfNeeded(node.getHost(), node.getPort(), throwable);
    }
}
