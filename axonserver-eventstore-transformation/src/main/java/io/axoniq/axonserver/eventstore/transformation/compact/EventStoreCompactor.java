package io.axoniq.axonserver.eventstore.transformation.compact;

import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
@FunctionalInterface
public interface EventStoreCompactor {


    /**
     * Deletes all events in the event store related to the given context.
     *
     * @param context the name of the context
     */
    Mono<Void> compact(String context);
}
