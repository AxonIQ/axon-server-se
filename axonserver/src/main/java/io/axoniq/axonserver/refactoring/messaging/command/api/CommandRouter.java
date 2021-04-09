package io.axoniq.axonserver.refactoring.messaging.command.api;

import org.springframework.security.core.Authentication;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandRouter {

    Mono<CommandResponse> dispatch(Authentication authentication, Command command);
}
