package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface TransformationApplier {

    interface Transformation {

        String id();

        String context();

        int version();

        long lastSequence();
    }

    /**
     * Returns a Mono that completes when the transformation is applied.
     * If the transformation is already partially applied, it will continue from the last applied sequence.
     *
     * @param transformation the transformation to be applied
     * @return a Mono that completes when the transformation is applied.
     */
    Mono<Void> apply(Transformation transformation);

    Mono<Long> lastAppliedSequence(String transformationId);
}
