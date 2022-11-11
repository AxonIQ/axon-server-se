package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CommandHandlerRegistry {

    Mono<Void> register(CommandHandlerSubscription handler);

    Mono<CommandHandler> unregister(String handlerId);

    Mono<CommandHandlerSubscription> handler(Command command);

    Flux<CommandHandler> all();

    CommandHandlerSubscription find(String handlerId);
}
