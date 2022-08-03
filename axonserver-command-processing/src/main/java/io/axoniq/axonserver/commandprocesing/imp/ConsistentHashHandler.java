package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerSubscribedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConsistentHashHandler implements HandlerSelector, CommandHandlerSubscribedInterceptor,
        CommandHandlerUnsubscribedInterceptor {

    private final static Logger logger = LoggerFactory.getLogger(ConsistentHashHandler.class);

    private final Map<CommandIdentifier, ConsistentHash> consistentHashes = new ConcurrentHashMap<>();
    private final Function<CommandHandler, Optional<Number>> loadFactorProvider;
    private final Function<Command, Optional<String>> routingKeyProvider;

    public ConsistentHashHandler(Function<CommandHandler, Optional<Number>> loadFactorProvider,
                                 Function<Command, Optional<String>> routingKeyProvider) {
        this.loadFactorProvider = loadFactorProvider;
        this.routingKeyProvider = routingKeyProvider;
    }

    @Override
    public Mono<Void> onCommandHandlerSubscribed(CommandHandler commandHandler) {
        return Mono.fromRunnable(() -> {
            Number loadFactor = loadFactorProvider.apply(commandHandler).orElse(100);
            logger.debug("{}[{}] ({}) subscribed load factor {}", commandHandler.commandName(),
                         commandHandler.context(),
                         commandHandler.id(),
                         loadFactor);

            CommandIdentifier key = new CommandIdentifier(commandHandler.commandName(),
                                                          commandHandler.context());
            consistentHashes.put(key, consistentHashes.computeIfAbsent(key, c -> new ConsistentHash())
                                                      .with(commandHandler.id(),
                                                            loadFactor.intValue()));
        });
    }

    @Override
    public Mono<Void> onCommandHandlerUnsubscribed(CommandHandler commandHandler) {
        return Mono.fromRunnable(() -> {
            logger.debug("{}[{}] ({}) unsubscribed%n", commandHandler.commandName(),
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

        logger.debug("{}[{}] Selecting based on consistent hash -  {} candidates%n", command.commandName(),
                     command.context(), candidates.size());
        Map<String, CommandHandlerSubscription> keys = candidates.stream()
                                                                 .collect(Collectors.toMap(s -> s.commandHandler().id(),
                                                                                           s -> s));
        String routingKey = routingKeyProvider.apply(command).orElse(null);
        if (routingKey == null) {
            return candidates;
        }
        CommandHandlerSubscription member = consistentHash.getMember(routingKey,
                                                                     keys.keySet())
                                                          .map(m -> keys.get(m.getClient()))
                                                          .orElse(null);
        if (member != null) {
            logger.debug("{}[{}] Selected {} - {}", command.commandName(),
                         command.context(), member.commandHandler().id(), routingKey);
            return Collections.singleton(member);
        }
        return candidates;
    }
}
