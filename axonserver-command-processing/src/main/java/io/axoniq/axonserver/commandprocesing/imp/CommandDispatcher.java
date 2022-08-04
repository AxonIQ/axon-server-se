package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import reactor.core.publisher.Mono;

public interface CommandDispatcher {

    Mono<CommandResult> dispatch(CommandHandlerSubscription handler, Command commandRequest);

    default void request(String clientId, long count) {

    }
}
