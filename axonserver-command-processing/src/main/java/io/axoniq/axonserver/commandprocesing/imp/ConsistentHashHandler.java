package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerSubscribedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ConsistentHashHandler implements HandlerSelector, CommandHandlerSubscribedInterceptor,
        CommandHandlerUnsubscribedInterceptor {

    private final Map<CommandIdentifier, ConsistentHash> consistentHashes = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> onCommandHandlerSubscribed(CommandHandler commandHandler) {
        return Mono.fromRunnable(() -> {
            Number loadFactor = (Number) commandHandler.metadata().metadataValue(CommandHandler.LOAD_FACTOR).block();
            System.out.printf("%s[%s] (%s) subscribed load factor %s%n", commandHandler.commandName(),
                              commandHandler.context(),
                              commandHandler.id(),
                              loadFactor);

            CommandIdentifier key = new CommandIdentifier(commandHandler.commandName(),
                                                          commandHandler.context());
            consistentHashes.put(key, consistentHashes.computeIfAbsent(key, c -> new ConsistentHash())
                                                      .with(commandHandler.id(),
                                                            loadFactor == null ? 100 : loadFactor.intValue()));
        });
    }

    @Override
    public Mono<Void> onCommandHandlerUnsubscribed(CommandHandler commandHandler) {
        return Mono.fromRunnable(() -> {
            System.out.printf("%s[%s] (%s) unsubscribed%n", commandHandler.commandName(),
                              commandHandler.context(), commandHandler.id());
            CommandIdentifier key = new CommandIdentifier(commandHandler.commandName(),
                                                          commandHandler.context());
            ConsistentHash consitentHash = consistentHashes.get(key);
            if (consitentHash != null) {
                consistentHashes.put(key, consitentHash.without(commandHandler.id()));
            }
        });
    }

    @Override
    public Set<CommandHandlerSubscription> select(Set<CommandHandlerSubscription> candidates, Command command) {
        ConsistentHash consistentHash = consistentHashes.get(new CommandIdentifier(command.commandName(),
                                                                                   command.context()));
        if (consistentHash == null) {
            return candidates;
        }

        System.out.printf("%s[%s] Selecting based on consistent hash -  %d candidates%n", command.commandName(),
                          command.context(), candidates.size());
        Map<String, CommandHandlerSubscription> keys = candidates.stream()
                                                                 .collect(Collectors.toMap(s -> s.commandHandler().id(),
                                                                                           s -> s));
        String routingKey = (String) command.metadata().metadataValue(Command.ROUTING_KEY).block();
        CommandHandlerSubscription member = consistentHash.getMember(routingKey,
                                                                     keys.keySet())
                                                          .map(m -> keys.get(m.getClient()))
                                                          .orElse(null);
        if (member != null) {
            System.out.printf("%s[%s] Selected %s - %s%n", command.commandName(),
                              command.context(), member.commandHandler().id(), routingKey);
            return Collections.singleton(member);
        }
        return candidates;
    }
}
