package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import reactor.core.publisher.Flux;

public interface HandlerSelector {

    Flux<CommandHandlerSubscription> select(Flux<CommandHandlerSubscription> candidates, Command command);
}
