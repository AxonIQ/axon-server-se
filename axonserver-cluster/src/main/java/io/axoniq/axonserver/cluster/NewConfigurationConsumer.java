package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Config;

/**
 * Consumes new configuration from RAFT messages.
 *
 * @author Milan Savic
 */
public interface NewConfigurationConsumer {

    /**
     * Applies new configuration.
     *
     * @param configuration new configuration to be applied
     */
    void consume(Config configuration);
}
