package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.message.ClientIdentification;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
public interface Rule {

    ConnectionValue apply(ClientIdentification client, String server);
}
