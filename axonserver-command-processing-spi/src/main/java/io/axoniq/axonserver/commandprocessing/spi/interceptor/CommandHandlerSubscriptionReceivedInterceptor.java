package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public interface CommandHandlerSubscriptionReceivedInterceptor extends Interceptor {

    Mono<Void> onCommandHandlerSubscriptionReceived(CommandHandler commandHandler,
                                                    Function<CommandHandler, Mono<Void>> proceed);
}
