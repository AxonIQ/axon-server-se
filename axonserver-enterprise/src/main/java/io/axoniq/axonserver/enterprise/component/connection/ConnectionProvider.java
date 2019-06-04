package io.axoniq.axonserver.enterprise.component.connection;

import io.axoniq.axonserver.message.ClientIdentification;

/**
 * Interface towards a mechanism that provides the best node for client to connect with.
 *
 * It is a functional interface, whose functional method is {@link #bestMatch(ClientIdentification, Iterable)}
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@FunctionalInterface
public interface ConnectionProvider {

    /**
     * Returns the node between the specified nodes that best fits the specified client.
     *
     * @param client the client
     * @param nodes  the candidates
     * @return the node between the specified nodes that best fits the specified client
     */
    String bestMatch(ClientIdentification client, Iterable<String> nodes);
}
