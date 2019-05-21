package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.grpc.Channel;

/**
 * Provider of {@link Channel}.
 *
 * It is a functional interface whose functional method is {@link #get(String, int)}.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@FunctionalInterface
public interface ChannelProvider {

    /**
     * Returns the {@link Channel} to communicate with the specified server
     *
     * @param hostname the hostname of the server
     * @param port     the port of the server
     * @return the channel
     */
    Channel get(String hostname, int port);

    /**
     * Returns the {@link Channel} to communicate with the specified server
     *
     * @param node the server to communicate with
     * @return the channel
     */
    default Channel get(ClusterNode node) {
        return get(node.getInternalHostName(), node.getGrpcInternalPort());
    }

    /**
     * Returns the {@link Channel} to communicate with the specified server
     *
     * @param node the server to communicate with
     * @return the channel
     */
    default Channel get(Node node) {
        return get(node.getHost(), node.getPort());
    }
}
