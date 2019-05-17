package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.grpc.Channel;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
@FunctionalInterface
public interface ChannelProvider {

    Channel get(String hostname, int port);

    default Channel get(ClusterNode node) {
        return get(node.getInternalHostName(), node.getGrpcInternalPort());
    }

    default Channel get(Node node) {
        return get(node.getHost(), node.getPort());
    }
}
