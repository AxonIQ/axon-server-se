package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventStoreLockProvider;
import io.axoniq.axonserver.util.IdLock;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LockingEventStoreTransformer implements EventStoreTransformer {

    private final EventStoreLockProvider eventStoreLockProvider;

    private final EventStoreTransformer delegate;

    public LockingEventStoreTransformer(EventStoreLockProvider eventStoreLockProvider,
                                        EventStoreTransformer delegate) {
        this.eventStoreLockProvider = eventStoreLockProvider;
        this.delegate = delegate;
    }


    @Override
    public Flux<Long> transformEvents(String context, int version, Flux<EventWithToken> transformedEvents) {
        return executeInLock(context, delegate.transformEvents(context, version, transformedEvents));
    }

    @Override
    public Mono<Void> compact(String context) {
        return executeInLock(context, delegate.compact(context)).then();
    }


    private <T, P extends Publisher<T>> Flux<T> executeInLock(String context, P action) {
        return Flux.usingWhen(Mono.<IdLock.Ticket>create(sink -> {
                                                             IdLock.Ticket ticket = eventStoreLockProvider
                                                                     .apply(context)
                                                                     .request("transformation");
                                                             if (ticket.isAcquired()) {
                                                                 sink.success(ticket);
                                                             } else {
                                                                 sink.error(new RuntimeException("unable to acquire Event Store lock"));
                                                             }
                                                         }
                              ),
                              ticket -> action,
                              ticket -> Mono.fromRunnable(ticket::release)
        );
    }
}
