package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import reactor.core.publisher.Mono;

public interface CommandHandlerUnsubscribedInterceptor extends Interceptor {

    Mono<Void> onCommandHandlerUnsubscribed(CommandHandler commandHandler);
}