package io.axoniq.axonserver.refactoring.messaging.command.api;

import io.axoniq.axonserver.refactoring.messaging.api.Registration;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandHandlerRegistry {

    Mono<Registration> register(CommandHandler commandHandler);

    List<CommandHandler> handlers();

    default List<CommandHandler> handlers(CommandDefinition commandDefinition) {
        return handlers().stream()
                         .filter(ch -> commandDefinition.equals(ch.definition()))
                         .collect(Collectors.toList());
    }
}
