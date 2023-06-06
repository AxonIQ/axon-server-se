package io.axoniq.axonserver.eventstore.transformation.clean;

import reactor.core.publisher.Flux;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface TransformationsToBeCleaned {

    Flux<TransformationIdentifier> get(); //todo rename
}
