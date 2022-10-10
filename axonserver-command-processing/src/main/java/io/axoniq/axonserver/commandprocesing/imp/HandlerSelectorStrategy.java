package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;

import java.util.Set;

public interface HandlerSelectorStrategy {

    Set<CommandHandlerSubscription> select(Set<CommandHandlerSubscription> candidates, Command command);
}
