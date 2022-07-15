package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public interface CommandReceivedInterceptor extends Interceptor {

    Mono<Void> onCommandReceived(Command command, Function<Command, Mono<Void>> proceed);
}
