package io.axoniq.axonserver.eventstore.transformation.compact;

import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface MarkEventStoreCompacted {

    Mono<Void> markCompacted(String compactionId, String context);
}
