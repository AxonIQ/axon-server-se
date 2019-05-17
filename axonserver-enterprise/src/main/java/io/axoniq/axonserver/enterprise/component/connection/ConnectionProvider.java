package io.axoniq.axonserver.enterprise.component.connection;

import io.axoniq.axonserver.message.ClientIdentification;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
@FunctionalInterface
public interface ConnectionProvider {

    String bestMatch(ClientIdentification client, Iterable<String> nodes);
}
