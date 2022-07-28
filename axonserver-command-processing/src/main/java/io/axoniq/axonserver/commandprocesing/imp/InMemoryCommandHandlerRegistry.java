package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class InMemoryCommandHandlerRegistry implements CommandHandlerRegistry {

    private final Map<CommandIdentifier, Set<CommandHandlerSubscription>> handlersPerCommand = new ConcurrentHashMap<>();
    private final Map<String, CommandHandlerSubscription> handlers = new ConcurrentHashMap<>();
    private final List<HandlerSelector> handlerSelectorList;

    public InMemoryCommandHandlerRegistry(List<HandlerSelector> handlerSelectorList) {
        this.handlerSelectorList = handlerSelectorList;
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
        return Mono.fromSupplier(() -> {
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
    public Mono<CommandHandlerSubscription> handler(Command command) {
        return Mono.fromCallable(() -> {
            Set<CommandHandlerSubscription> handlers =
                    handlersPerCommand.getOrDefault(new CommandIdentifier(command.commandName(),
                                                                          command.context()),
                                                    Collections.emptySet());
            return selectSubscription(handlers, command).orElseThrow(NoHandlerFoundException::new);
        });
    }

    @Override
    public Flux<CommandHandler> all() {
        return Flux.fromIterable(handlers.values()).map(CommandHandlerSubscription::commandHandler);
    }

    private Optional<CommandHandlerSubscription> selectSubscription(Set<CommandHandlerSubscription> handlers,
                                                                    Command command) {
        if (handlers.isEmpty()) {
            return Optional.empty();
        }

        if (handlers.size() == 1) {
            return Optional.of(first(handlers));
        }

        for (HandlerSelector selector : handlerSelectorList) {
            handlers = selector.select(handlers, command);
            if (handlers.isEmpty()) {
                return Optional.empty();
            }

            if (handlers.size() == 1) {
                return Optional.of(first(handlers));
            }
        }

        return Optional.of(first(handlers));
    }

    private CommandHandlerSubscription first(Set<CommandHandlerSubscription> handlers) {
        if (handlers.isEmpty()) {
            throw new IllegalStateException();
        }
        return handlers.iterator().next();
    }
}
