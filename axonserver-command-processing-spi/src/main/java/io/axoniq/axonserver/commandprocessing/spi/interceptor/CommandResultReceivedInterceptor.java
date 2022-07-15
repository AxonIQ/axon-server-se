package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public interface CommandResultReceivedInterceptor extends Interceptor {

    Mono<Void> onCommandResultReceived(CommandResult commandResult, Function<CommandResult, Mono<Void>> proceed);
}
