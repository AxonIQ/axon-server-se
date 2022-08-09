package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandException;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;
import io.axoniq.axonserver.commandprocessing.spi.Registration;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandFailedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerSubscribedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandReceivedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandResultReceivedInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DefaultCommandRequestProcessor implements CommandRequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommandRequestProcessor.class);
    private final CommandHandlerRegistry commandHandlerRegistry;
    private final CommandDispatcher commandDispatcher;

    private final Map<Class<? extends Interceptor>, List<Interceptor>> interceptorMap = new ConcurrentHashMap<>();

    public DefaultCommandRequestProcessor() {
        this(Collections.emptyList());
    }

    public DefaultCommandRequestProcessor(List<HandlerSelector> handlerSelectorList) {
        this(new InMemoryCommandHandlerRegistry(handlerSelectorList), new DirectCommandDispatcher());
    }

    public DefaultCommandRequestProcessor(CommandHandlerRegistry commandHandlerRegistry,
                                          CommandDispatcher commandDispatcher) {
        this.commandHandlerRegistry = commandHandlerRegistry;
        this.commandDispatcher = commandDispatcher;
    }

    @Override
    public Mono<Void> register(CommandHandlerSubscription handler) {
        return commandHandlerRegistry.register(handler)
                                     .then(invokeHooks(CommandHandlerSubscribedInterceptor.class,
                                                       i -> i.onCommandHandlerSubscribed(handler.commandHandler())).then());
    }

    @Override
    public Mono<Void> unregister(String handlerId) {
        return commandHandlerRegistry.unregister(handlerId)
                                     .flatMap(handler -> invokeHooks(CommandHandlerUnsubscribedInterceptor.class,
                                                                     i -> i.onCommandHandlerUnsubscribed(handler))
                                             .then());
    }


    private <T> Flux<Void> invokeHooks(Class<T> clazz, Function<T, Mono<Void>> interceptor) {
        return Flux.fromIterable(interceptorMap.getOrDefault(clazz,
                                                             Collections.emptyList()))
                   .concatMap(i -> interceptor.apply((T) i));
    }

    private <T, R> Mono<R> invokeInterceptors(Class<T> clazz, Mono<R> initial,
                                              BiFunction<T, Mono<R>, Mono<R>> interceptor) {
        return Flux.fromIterable(interceptorMap.getOrDefault(clazz,
                                                             Collections.emptyList()))
                   .reduce(initial, (value, i) -> interceptor.apply((T) i, value))
                   .flatMap(Mono::from);
    }

    @Override
    public Mono<CommandResult> dispatch(Command commandRequest) {
        return invokeInterceptors(CommandReceivedInterceptor.class,
                                  Mono.just(commandRequest),
                                  CommandReceivedInterceptor::onCommandReceived)
                .flatMap(command -> commandHandlerRegistry.handler(command)
                                                          .flatMap(subscription -> commandDispatcher.dispatch(
                                                                                                            subscription,
                                                                                                            command)
                                                                                                    .flatMap(
                                                                                                            commandResult -> invokeInterceptors(
                                                                                                                    CommandResultReceivedInterceptor.class,
                                                                                                                    Mono.just(
                                                                                                                            commandResult),
                                                                                                                    CommandResultReceivedInterceptor::onCommandResultReceived)
                                                                                                                    .thenReturn(commandResult))
                                                                                                    .name("commandDispatch")
                                                                                                    .tag("command",
                                                                                                        command
                                                                                                                       .commandName())
                                                                                                    .tag("context",
                                                                                                            command
                                                                                                                       .context())
                                                                                                    .tag("source",
                                                                                                            command
                                                                                                                       .metadata()
                                                                                                                       .metadataValue(
                                                                                                                               Command.CLIENT_ID,
                                                                                                                               "NO-SOURCE"))
                                                                                                    .tag("target",
                                                                                                         subscription.commandHandler()
                                                                                                                     .metadata()
                                                                                                                     .metadataValue(
                                                                                                                             Command.CLIENT_ID,
                                                                                                                             "NO-SOURCE"))
                                                                                                    .metrics()

                                                          ))
                .onErrorResume(
                        throwable -> invokeInterceptors(
                                CommandFailedInterceptor.class,
                                commandFailed(
                                        commandRequest,
                                        throwable),
                                CommandFailedInterceptor::onCommandFailed)
                                .name("commandDispatchErrors")
                                .tag("command",
                                        commandRequest
                                                   .commandName())
                                .tag("context",
                                        commandRequest
                                                   .context())
                                .tag("source",
                                        commandRequest
                                                   .metadata()
                                                   .metadataValue(
                                                           Command.CLIENT_ID,
                                                           "NO-SOURCE"))
                                .tag("error", throwable.toString())
                                .metrics()
                                .then(Mono.error(
                                        throwable)));
    }

    private Mono<CommandException> commandFailed(Command command, Throwable throwable) {
        return Mono.just(new CommandException() {
            @Override
            public Command command() {
                return command;
            }

            @Override
            public Throwable exception() {
                return throwable;
            }
        });
    }

    @Override
    public <T extends Interceptor> Registration registerInterceptor(Class<T> type, T interceptor) {
        logger.debug("Register {} {}", type.getSimpleName(), interceptor);
        List<Interceptor> interceptors = interceptorMap.computeIfAbsent(type, t -> new CopyOnWriteArrayList<>());
        interceptors.add(interceptor);
        interceptors.sort(Comparator.comparingLong(Interceptor::priority));
        return () -> Mono.fromRunnable(() -> interceptorMap.getOrDefault(type, Collections.emptyList())
                                                           .remove(interceptor));
    }
}
