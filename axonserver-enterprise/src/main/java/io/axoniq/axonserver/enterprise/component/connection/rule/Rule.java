package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.message.ClientIdentification;

/**
 * Represents a rule to calculate the {@link ConnectionValue} between a client and a node of the cluster
 *
 * It is a {@link FunctionalInterface}, whose functional method is {@link #apply(ClientIdentification, String)}
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@FunctionalInterface
public interface Rule {

    /**
     * Returns the {@link ConnectionValue} between the specified client and server
     *
     * @param client the client
     * @param server the axon server node instance
     * @return the {@link ConnectionValue}
     */
    ConnectionValue apply(ClientIdentification client, String server);
}
