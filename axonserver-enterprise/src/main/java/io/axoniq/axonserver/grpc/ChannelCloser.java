package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.Node;

/**
 * Functional interface responsible to close the {@link io.grpc.Channel} between the local instance 
 * and another AxonServer instance, if it is no more usable.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public interface ChannelCloser {

    /**
     * Shutdowns the {@link io.grpc.Channel} with the specified AxonServer instance if it is no longer usable.
     *
     * @param hostname  the hostname of the other AxonServer instance
     * @param port      the gRPC port used by the channel to connect to AxonServer instance
     * @param throwable the exception that requires the shutdown check
     * @return true if the {@link io.grpc.Channel} has been shutdown, false otherwise
     */
    boolean shutdownIfNeeded(String hostname, int port, Throwable throwable);

    /**
     * Shutdowns the {@link io.grpc.Channel} with the specified AxonServer instance if it is no more usable.
     *
     * @param node      the other AxonServer instance
     * @param throwable the exception that requires the shutdown check
     * @return true if the {@link io.grpc.Channel} has been shutdown, false otherwise
     */
    default boolean shutdownIfNeeded(ClusterNode node, Throwable throwable) {
        return shutdownIfNeeded(node.getInternalHostName(), node.getGrpcInternalPort(), throwable);
    }

    /**
     * Shutdowns the {@link io.grpc.Channel} with the specified AxonServer instance if it is no more usable.
     *
     * @param node the other AxonServer instance
     * @param throwable the exception that requires the shutdown check
     * @return true if the {@link io.grpc.Channel} has been shutdown, false otherwise
     */
    default boolean shutdownIfNeeded(Node node, Throwable throwable) {
        return shutdownIfNeeded(node.getHost(), node.getPort(), throwable);
    }
}
