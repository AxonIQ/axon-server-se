package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.CommandException;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import reactor.core.publisher.Mono;

public interface CommandFailedInterceptor extends Interceptor {

    Mono<CommandException> onCommandFailed(Mono<CommandException> commandException);
}
