package io.axoniq.axonserver.refactoring.messaging.command.api;

import io.axoniq.axonserver.refactoring.messaging.api.Message;

import java.util.Optional;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandResponse {

    Message message();

    Optional<Error> error();
}
