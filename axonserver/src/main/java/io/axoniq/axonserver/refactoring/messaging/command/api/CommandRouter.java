package io.axoniq.axonserver.refactoring.messaging.command.api;

import io.axoniq.axonserver.refactoring.api.Authentication;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandRouter {

    Mono<CommandResponse> dispatch(Authentication authentication, Command command);
}
