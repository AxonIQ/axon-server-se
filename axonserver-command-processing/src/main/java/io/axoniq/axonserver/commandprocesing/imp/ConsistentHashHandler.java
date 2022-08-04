package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerSubscribedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

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
    public Flux<CommandHandlerSubscription> select(Flux<CommandHandlerSubscription> candidates, Command command) {
        return Mono.justOrEmpty(consistentHashes.get(new CommandIdentifier(command.commandName(),
                        command.context())))
                .flatMapMany(consistentHash -> selectBasedOnCache(candidates, command, consistentHash))
                .switchIfEmpty(candidates);
    }

    private Mono<CommandHandlerSubscription> selectBasedOnCache(Flux<CommandHandlerSubscription> candidates, Command command, ConsistentHash consistentHash) {
        String routingKey = routingKeyProvider.apply(command).orElse(null);
        if (routingKey == null) {
            return Mono.empty();
        }

        logger.debug("{}[{}] Selecting based on consistent hash", command.commandName(),
                command.context());

        return candidatesMap(candidates)
                .flatMap(candidatesEntries -> getClient(consistentHash, routingKey, candidatesEntries))
                .doOnNext(member -> logger.debug("{}[{}] Selected {} - {}", command.commandName(),
                        command.context(), member.commandHandler().id(), routingKey));
    }

    private Mono<CommandHandlerSubscription> getClient(ConsistentHash consistentHash, String routingKey, Map<String, CommandHandlerSubscription> candidatesEntries) {
        return Mono.justOrEmpty(consistentHash.getMember(routingKey,
                        candidatesEntries.keySet()))
                .map(m -> candidatesEntries.get(m.getClient()));
    }

    private Mono<Map<String, CommandHandlerSubscription>> candidatesMap(Flux<CommandHandlerSubscription> candidates) {
        return candidates.collectMap(s -> s.commandHandler().id(), s -> s);
    }
}
