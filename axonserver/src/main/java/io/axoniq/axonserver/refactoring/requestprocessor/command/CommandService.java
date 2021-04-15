package io.axoniq.axonserver.refactoring.requestprocessor.command;

import io.axoniq.axonserver.refactoring.api.Authentication;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandService {

    Mono<CommandResponse> execute(Command command, Authentication authentication);
}
