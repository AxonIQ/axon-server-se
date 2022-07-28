package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DirectCommandDispatcher implements CommandDispatcher {

    private final Logger logger = LoggerFactory.getLogger(DirectCommandDispatcher.class);

    @Override
    public Mono<CommandResult> dispatch(CommandHandlerSubscription handler, Command commandRequest) {
        logger.debug("{}: dispatch {} ({}) to {}", commandRequest.context(), commandRequest.commandName(),
                     commandRequest.id(), handler.commandHandler().id());
        return handler.dispatch(commandRequest).doOnNext(r -> {
            logger.debug("{}: received result for {} ({})", commandRequest.context(), commandRequest.commandName(),
                         commandRequest.id());
        });
    }
}
