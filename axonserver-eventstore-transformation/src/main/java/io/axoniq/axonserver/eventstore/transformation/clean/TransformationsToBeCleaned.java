package io.axoniq.axonserver.eventstore.transformation.clean;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TransformationsToBeCleaned {

    Flux<TransformationToBeCleaned> toBeCleaned();

    interface TransformationToBeCleaned {

        String context();

        String id();

        Mono<Void> markAsCleaned();
    }
}
