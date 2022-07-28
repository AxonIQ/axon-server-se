package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import reactor.core.publisher.Mono;

public interface CommandResultReceivedInterceptor extends Interceptor {

    Mono<CommandResult> onCommandResultReceived(Mono<CommandResult> commandResult);
}
