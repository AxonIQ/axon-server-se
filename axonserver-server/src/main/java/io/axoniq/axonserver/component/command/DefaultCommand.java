package io.axoniq.axonserver.component.command;

import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.serializer.Media;

import java.util.Set;

/**
 * Created by Sara Pellegrini on 20/03/2018.
 * sara.pellegrini@gmail.com
 */
class DefaultCommand implements ComponentCommand {

    private final CommandRegistrationCache.RegistrationEntry command;

    private final Set<CommandHandler> commandHandlers;

    public DefaultCommand(CommandRegistrationCache.RegistrationEntry command, Set<CommandHandler> commandHandlers) {
        this.command = command;
        this.commandHandlers = commandHandlers;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return commandHandlers.stream().anyMatch(handler -> component.equals(handler.getComponentName()));
    }

    @Override
    public boolean belongsToContext(String context) {
        return command.getContext().equals(context);
    }

    @Override
    public void printOn(Media media) {
        media.with("name", command.getCommand());
    }
}