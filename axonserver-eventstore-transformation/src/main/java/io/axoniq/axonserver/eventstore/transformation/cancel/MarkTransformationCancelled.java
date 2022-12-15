package io.axoniq.axonserver.eventstore.transformation.cancel;

import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface MarkTransformationCancelled {

    Mono<Void> markCancelled(String context, String transformationId);
}
