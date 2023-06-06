package io.axoniq.axonserver.eventstore.transformation.apply;

import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface CleanTransformationApplied {

    Mono<Void> clean();
}
