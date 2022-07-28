package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import reactor.core.publisher.Mono;

public interface CommandReceivedInterceptor extends Interceptor {

    Mono<Command> onCommandReceived(Mono<Command> command);
}
