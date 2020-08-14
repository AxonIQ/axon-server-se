package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.message.ClientStreamIdentification;

/**
 * {@link Iterable} of {@link ClientStreamIdentification} for Axon Framework clients.
 *
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
public interface ClientIdentifications extends Iterable<ClientStreamIdentification> {

}
