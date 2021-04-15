package io.axoniq.axonserver.refactoring.requestprocessor;

import io.axoniq.axonserver.refactoring.api.Authentication;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandRouter;
import io.axoniq.axonserver.refactoring.requestprocessor.command.CommandService;
import io.axoniq.axonserver.refactoring.security.AuditLog;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since
 */
@Service
public class CommandRequestProcessor implements CommandService {

    private static final Logger auditLog = AuditLog.getLogger();
    private final CommandRouter commandRouter;

    public CommandRequestProcessor(CommandRouter commandRouter) {
        this.commandRouter = commandRouter;
    }


    @Override
    public Mono<CommandResponse> execute(Command command, Authentication authentication) {
            auditLog.info("[{}] Request to dispatch a \"{}\" Command.", authentication.name(), command.definition().name());
            return commandRouter.dispatch(authentication, command);
    }
}
