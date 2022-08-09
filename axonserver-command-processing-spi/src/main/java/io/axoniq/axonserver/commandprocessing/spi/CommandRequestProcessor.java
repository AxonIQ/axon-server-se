package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Mono;

public interface CommandRequestProcessor {

    Mono<Void> register(CommandHandlerSubscription handler);

    Mono<Void> unregister(String handlerId);

    Mono<CommandResult> dispatch(CommandRequest command);

    <T extends Interceptor> Registration registerInterceptor(Class<T> type, T interceptor);
}