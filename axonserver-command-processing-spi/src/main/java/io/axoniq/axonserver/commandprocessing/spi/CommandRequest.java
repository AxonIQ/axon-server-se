package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Mono;

import java.io.Serializable;

public interface CommandRequest extends Serializable {

    Command command();

    Mono<Void> complete();

    Mono<Void> complete(CommandResult result);

    Mono<Void> completeExceptionally(Throwable t);
}
