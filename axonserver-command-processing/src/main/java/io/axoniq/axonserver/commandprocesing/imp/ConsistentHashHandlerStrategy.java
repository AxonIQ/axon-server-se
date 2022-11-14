/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConsistentHashHandlerStrategy implements HandlerSelectorStrategy, CommandHandlerSubscribedInterceptor,
        CommandHandlerUnsubscribedInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashHandlerStrategy.class);

    private final Map<CommandIdentifier, ConsistentHash> consistentHashes = new ConcurrentHashMap<>();
    private final Function<CommandHandler, Number> loadFactorProvider;
    private final Function<Command, String> routingKeyProvider;
    private final Function<CommandHandler, String> hashKeySourceProvider;

    public ConsistentHashHandlerStrategy(Function<CommandHandler, Number> loadFactorProvider,
                                         Function<CommandHandler, String> hashKeySourceProvider,
                                         Function<Command, String> routingKeyProvider) {
        this.loadFactorProvider = loadFactorProvider;
        this.routingKeyProvider = routingKeyProvider;
        this.hashKeySourceProvider = hashKeySourceProvider;
    }

    @Override
    public Mono<Void> onCommandHandlerSubscribed(CommandHandler commandHandler) {
        return Mono.fromRunnable(() -> {
            Number loadFactor = loadFactorProvider.apply(commandHandler);
            logger.debug("{}[{}] ({}) subscribed load factor {}", commandHandler.commandName(),
                         commandHandler.context(),
                         commandHandler.id(),
                         loadFactor);

            CommandIdentifier key = new CommandIdentifier(commandHandler.commandName(),
                                                          commandHandler.context());
            consistentHashes.put(key, consistentHashes.computeIfAbsent(key, c -> new ConsistentHash())
                                                      .with(hash(commandHandler),
                                                            loadFactor.intValue()));
        });
    }

    @Override
    public Mono<Void> onCommandHandlerUnsubscribed(CommandHandler commandHandler) {
        return Mono.fromRunnable(() -> {
            logger.debug("{}[{}] ({}) unsubscribed", commandHandler.commandName(),
                         commandHandler.context(), commandHandler.id());
            CommandIdentifier key = new CommandIdentifier(commandHandler.commandName(),
                                                          commandHandler.context());
            ConsistentHash consitentHash = consistentHashes.get(key);
            if (consitentHash != null) {
                consistentHashes.put(key, consitentHash.without(hash(commandHandler)));
            }
        });
    }

    private String hash(CommandHandler commandHandler) {
        return hashKeySourceProvider.apply(commandHandler);
    }

    @Override
    public Set<CommandHandlerSubscription> select(Set<CommandHandlerSubscription> candidates, Command command) {
        ConsistentHash consistentHash = consistentHashes.get(new CommandIdentifier(command.commandName(),
                                                                                   command.context()));
        if (consistentHash == null) {
            return candidates;
        }

        logger.debug("{}[{}] Selecting based on consistent hash -  {} candidates", command.commandName(),
                     command.context(), candidates.size());
        Map<String, CommandHandlerSubscription> keys = candidates.stream()
                                                                 .collect(Collectors.toMap(s -> hash(s.commandHandler()),
                                                                                           s -> s));
        String routingKey = routingKeyProvider.apply(command);
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
