package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.NoHandlerFoundException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;

public class InMemoryCommandHandlerRegistry implements CommandHandlerRegistry {

    private final Map<CommandIdentifier, Set<CommandHandlerSubscription>> handlersPerCommand = new ConcurrentHashMap<>();
    private final Map<String, CommandHandlerSubscription> handlers = new ConcurrentHashMap<>();
    private final List<HandlerSelectorStrategy> handlerSelectorStrategyList;

    public InMemoryCommandHandlerRegistry(List<HandlerSelectorStrategy> handlerSelectorStrategyList) {
        this.handlerSelectorStrategyList = handlerSelectorStrategyList;
    }

    @Override
    public Mono<Void> register(CommandHandlerSubscription handler) {
        return Mono.fromRunnable(() -> {
            handlersPerCommand.computeIfAbsent(new CommandIdentifier(handler.commandHandler().commandName(),
                                                                     handler.commandHandler().context()),
                                               h -> new CopyOnWriteArraySet<>()).add(handler);
            handlers.put(handler.commandHandler().id(), handler);
        });
    }

    @Override
    public Mono<CommandHandler> unregister(String handlerId) {
        return Mono.fromCallable(() -> {
            CommandHandlerSubscription handler = handlers.remove(handlerId);
            if (handler == null) {
                return null;
            }
            handlersPerCommand.computeIfPresent(new CommandIdentifier(handler.commandHandler().commandName(),
                                                                      handler.commandHandler().context()),
                                                (key, old) -> {
                                                    old.remove(handler);
                                                    return old.isEmpty() ? null : old;
                                                });
            return handler.commandHandler();
        });
    }

    @Override
    public CommandHandlerSubscription find(String handlerId) {
        return handlers.get(handlerId);
    }

    @Override
    public Mono<CommandHandlerSubscription> handler(Command command) {
        return Mono.defer(() -> {
            Set<CommandHandlerSubscription> handlers =
                    handlersPerCommand.getOrDefault(new CommandIdentifier(command.commandName(),
                                                                          command.context()),
                                                    Collections.emptySet());
            return selectSubscription(handlers, command);
        });
    }

    @Override
    public Flux<CommandHandler> all() {
        return Flux.fromIterable(handlers.values()).map(CommandHandlerSubscription::commandHandler);
    }

    private Mono<CommandHandlerSubscription> selectSubscription(Set<CommandHandlerSubscription> handlers,
                                                                Command command) {
        return Flux.fromIterable(handlerSelectorStrategyList)
                   .reduce(handlers, (handlerSubscriptionSet, selector) ->
                           applySelector(command, selector, handlerSubscriptionSet))
                   .flatMapIterable(Function.identity())
                   .next()
                   .switchIfEmpty(Mono.error(new NoHandlerFoundException()));
    }

    private Set<CommandHandlerSubscription> applySelector(Command command, HandlerSelectorStrategy selector,
                                                          Set<CommandHandlerSubscription> handlers) {
        if (handlers.size() <= 1) {
            return handlers;
        }
        return selector.select(handlers, command);
    }
}
