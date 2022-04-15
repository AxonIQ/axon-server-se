package io.axoniq.axonserver.localstorage.transformation;

import reactor.core.publisher.Mono;

public interface LocalTransformationApplier {

    interface Transformation {

        String id();

        String context();

        int version();

        long lastSequence();
    }

    Mono<Void> apply(Transformation transformation);

    Mono<Long> lastAppliedSequence(String transformationId);
}
