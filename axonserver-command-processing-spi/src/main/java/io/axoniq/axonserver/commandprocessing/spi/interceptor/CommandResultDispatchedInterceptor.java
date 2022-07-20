package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import reactor.core.publisher.Mono;

public interface CommandResultDispatchedInterceptor extends Interceptor {

    Mono<Void> onCommandResultDispatched(CommandResult commandResult, Command command, CommandHandler commandHandler);
}
