package io.axoniq.axonserver.refactoring.requestprocessor;

import io.axoniq.axonserver.refactoring.api.Authentication;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandRouter;
import io.axoniq.axonserver.refactoring.requestprocessor.command.CommandService;
import io.axoniq.axonserver.refactoring.security.AuditLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since
 */
@Service
public class CommandRequestProcessor implements CommandService {

    private static final Logger logger = LoggerFactory.getLogger(CommandRequestProcessor.class);
    private static final Logger auditLog = AuditLog.getLogger();
    private final CommandRouter commandRouter;

    public CommandRequestProcessor(CommandRouter commandRouter) {
        this.commandRouter = commandRouter;
    }


    @Override
    public Mono<CommandResponse> execute(Command command, Authentication authentication) {
        final String commandName = command.definition().name();
        if (logger.isTraceEnabled()) {
            logger.trace("{}: Received command: {}", command.requester().id(), commandName);
        }
        auditLog.info("[{}] Request to dispatch a \"{}\" Command.", authentication.name(), commandName);
        // TODO: 4/16/2021 validate a command message
        return commandRouter.dispatch(authentication, command);
    }
}
