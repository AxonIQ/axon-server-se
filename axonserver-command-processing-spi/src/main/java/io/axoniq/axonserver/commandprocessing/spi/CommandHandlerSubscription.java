package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Mono;

import java.io.Serializable;

public interface CommandHandlerSubscription extends Serializable {

    CommandHandler commandHandler();

    Mono<CommandResult> dispatch(Command command);
}
