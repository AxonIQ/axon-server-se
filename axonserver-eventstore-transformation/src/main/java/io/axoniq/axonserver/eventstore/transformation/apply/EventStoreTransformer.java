package io.axoniq.axonserver.eventstore.transformation.apply;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import reactor.core.publisher.Flux;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
@FunctionalInterface
public interface EventStoreTransformer {

    /**
     * Transforms events in the event store. Returns a {@link Flux} of long, that represents the number of actions done
     * when the transformation function reaches a certain milestone. The flux is completed when the transformation is
     * completed.
     *
     * @param context           the name of the context for the event store
     * @param version           the new version number for the event store
     * @param transformedEvents a {@link Flux} of transformed events
     * @return a flux of progress in terms of number of events transformed
     */
    Flux<Long> transformEvents(String context, int version, Flux<EventWithToken> transformedEvents);
}
