package io.axoniq.axonserver.refactoring.messaging.command.api;

import io.axoniq.axonserver.refactoring.messaging.api.Registration;

import java.util.List;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandRegistry {

    Registration register(CommandHandler commandHandler);

    List<CommandHandler> handlers();
}
