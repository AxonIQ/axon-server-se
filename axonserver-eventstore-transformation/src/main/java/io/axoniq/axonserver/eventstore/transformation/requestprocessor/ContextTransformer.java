package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

public interface ContextTransformer {

    Mono<Void> start(String id, String description);

    Mono<Void> deleteEvent(String transformationId, long token, long sequence);

    Mono<Void> replaceEvent(String transformationId, long token, Event event, long sequence);

    Mono<Void> startCancelling(String transformationId);

    Mono<Void> markCancelled(String transformationId);

    Mono<Void> startApplying(String transformationId, long sequence, String applier);

    Mono<Void> markApplied(String transformationId);

    Mono<Void> markCompacted(String compactionId);

    Mono<Void> compact(String compactionId);
}
