package io.axoniq.axonserver.refactoring.messaging.command.api;

import io.axoniq.axonserver.refactoring.client.instance.Client;
import io.axoniq.axonserver.refactoring.messaging.api.ContextAware;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandHandler extends ContextAware {

    @Override
    default String context() {
        return definition().context();
    }

    CommandDefinition definition();

    Client client();

    Mono<CommandResponse> handle(Command command);
}
