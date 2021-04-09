package io.axoniq.axonserver.refactoring.messaging.query.api;

import io.axoniq.axonserver.refactoring.messaging.api.Message;

import java.util.Optional;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface QueryResponse {

    Message message();

    Optional<Error> error();
}
