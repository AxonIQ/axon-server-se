package io.axoniq.axonserver.enterprise.component.connection;

import io.axoniq.axonserver.message.ClientIdentification;

import java.util.List;

/**
 * Interface towards a mechanism that provides candidate nodes which are best node for client to connect with.
 *
 * It is a functional interface, whose functional method is {@link #bestMatches(ClientIdentification, Iterable)}
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@FunctionalInterface
public interface ConnectionProvider {

    /**
     * Returns a list of nodes which best fit the specified client. If multiple nodes have equal suitability, then all
     * candidate nodes are returned.
     *
     * @param client the client
     * @param nodes  the candidates
     * @return a list of nodes which best fit the specified client
     */
    List<String> bestMatches(ClientIdentification client, Iterable<String> nodes);
}
