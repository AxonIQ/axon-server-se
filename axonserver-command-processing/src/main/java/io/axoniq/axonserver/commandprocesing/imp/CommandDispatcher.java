package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import reactor.core.publisher.Mono;

public interface CommandDispatcher extends CommandHandlerUnsubscribedInterceptor {

    Mono<CommandResult> dispatch(CommandHandlerSubscription handler, Command commandRequest);

    default void request(String clientId, long count) {
    }
}
