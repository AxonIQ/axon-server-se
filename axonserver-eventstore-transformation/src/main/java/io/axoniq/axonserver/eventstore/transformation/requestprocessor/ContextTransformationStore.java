package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ContextTransformationStore {

    Flux<TransformationState> transformations();

    Mono<TransformationState> create();

    Mono<TransformationState> transformation(String id);

    Mono<Void> save(TransformationState transformation);
}
