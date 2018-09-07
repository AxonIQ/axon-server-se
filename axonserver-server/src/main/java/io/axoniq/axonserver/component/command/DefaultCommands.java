package io.axoniq.axonserver.component.command;

import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.command.CommandRegistrationCache.RegistrationEntry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 19/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DefaultCommands implements Iterable<ComponentCommand> {
    
    private final CommandRegistrationCache registrationCache;

    public DefaultCommands(CommandRegistrationCache registrationCache) {
        this.registrationCache = registrationCache;
    }

    @Override
    public Iterator<ComponentCommand> iterator() {
        Map<CommandHandler, Set<RegistrationEntry>> all = registrationCache.getAll();

        Map<RegistrationEntry, Set<CommandHandler>> commands = new HashMap<>();
        all.forEach((handler, registrations) -> registrations.forEach(registration -> {
                Set<CommandHandler> handlers = commands.computeIfAbsent(registration,
                                                                        c -> new HashSet<>());
                handlers.add(handler);
        }));

        return commands.entrySet().stream().map(e -> (ComponentCommand) new DefaultCommand(e.getKey(), e.getValue())).iterator();
    }
}
