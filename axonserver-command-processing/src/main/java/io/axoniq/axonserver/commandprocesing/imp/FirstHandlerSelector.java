package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;

import java.util.Collections;
import java.util.Set;

public class FirstHandlerSelector implements HandlerSelector {

    @Override
    public Set<CommandHandlerSubscription> select(Set<CommandHandlerSubscription> candidates, Command command) {
        System.out.printf("%s[%s] Selecting first handler%n", command.commandName(),
                          command.context());

        if (candidates.size() > 1) {
            return Collections.singleton(candidates.iterator().next());
        }
        return candidates;
    }
}
