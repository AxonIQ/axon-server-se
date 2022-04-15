package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface ContextTransformationStore {

    Mono<TransformationState> create();

    Mono<TransformationState> transformation(String id);

    Mono<TransformationState> current();

    Mono<Void> save(TransformationState transformation);
}
