package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.message.command.hashing.ConsistentHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Marc Gathier
 */
@Component("CommandRegistrationCache")
public class CommandRegistrationCache {
    private final Logger logger = LoggerFactory.getLogger(CommandRegistrationCache.class);

    private final ConcurrentMap<String, CommandHandler> contextForClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<RegistrationEntry>> registrationsPerClient = new ConcurrentHashMap<>();
    private final AtomicReference<ConsistentHash> consistentHashRef = new AtomicReference<>(new ConsistentHash());

    public void remove(String client) {
        consistentHashRef.updateAndGet(c-> c.without(client));
        contextForClient.remove(client);
        registrationsPerClient.remove(client);
    }

    public CommandHandler remove(String context, String command, String client) {
        CommandHandler commandHandler = contextForClient.get(client);
        Set<RegistrationEntry> registrations = registrationsPerClient.computeIfPresent(client, (c, set) -> {
            set.remove(new RegistrationEntry(context,command));
            if (set.isEmpty()) {
                return null;
            }
            return set;
        });
        if( registrations == null) {
            remove(client);
        }
        return commandHandler;
    }

    public void add(String context, String command, CommandHandler commandHandler) {
        consistentHashRef.updateAndGet(c -> c.with(commandHandler.getClient(), 100, cmd -> registrationsPerClient.get(commandHandler.getClient()).contains(cmd)));
        registrationsPerClient.computeIfAbsent(commandHandler.getClient(), key ->new CopyOnWriteArraySet<>()).add(new RegistrationEntry(context, command));
        contextForClient.put(commandHandler.getClient(), commandHandler);
    }

    public Map<CommandHandler, Set<RegistrationEntry>> getAll() {
        Map<CommandHandler, Set<RegistrationEntry>> resultMap = new HashMap<>();
        registrationsPerClient.forEach((client, commands) -> resultMap.put(contextForClient.get(client),commands));
        return resultMap;
    }

    public Set<RegistrationEntry> getCommandsFor(String clientNode) {
        return registrationsPerClient.get(clientNode);
    }

    public CommandHandler getNode(String context, Command request, String routingKey) {
        String client = consistentHashRef.get().getMember(routingKey, new RegistrationEntry(context, request.getName())).map(ConsistentHash.ConsistentHashMember::getClient).orElse(null);
        if( client == null ) return null;
        return contextForClient.get(client);
    }

    public CommandHandler findByClientAndCommand(String clientName, Command request) {
        String client = registrationsPerClient.entrySet().stream()
                .filter(e -> e.getKey().equals(clientName))
                .filter(e -> containsCommand(e.getValue(), request.getName()))
                .map(Map.Entry::getKey)
                .findFirst().orElse(null);
        if( client == null) return null;
        return contextForClient.get(client);

    }

    private boolean containsCommand(Set<RegistrationEntry> value, String command) {
        for( RegistrationEntry entry :value) {
            if( entry.getCommand().equals(command)) return true;
        }
        return false;
    }

    public String getComponentNameFor(String client) {
        if( contextForClient.containsKey(client)) {
            return contextForClient.get(client).getComponentName();
        }
        return null;
    }

    public static class RegistrationEntry {
        private final String command;
        private final String context;


        public RegistrationEntry(String context, String command) {
            this.command = command;
            this.context = context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegistrationEntry that = (RegistrationEntry) o;
            return Objects.equals(command, that.command) &&
                    Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(command, context);
        }

        public String getCommand() {
            return command;
        }

        public String getContext() {
            return context;
        }
    }
}
