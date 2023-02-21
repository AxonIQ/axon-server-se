package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface TransformationStateConverter {

    Mono<Transformation> from(TransformationState state);
}
