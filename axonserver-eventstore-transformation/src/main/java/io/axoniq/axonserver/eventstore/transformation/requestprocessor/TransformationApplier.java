package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface TransformationApplier {

    interface Transformation {



        String id();

        String context();

        int version();

        long lastSequence();

        Mono<Void> markAsApplied();
    }

    /**
     * Returns a Mono that completes when the transformation is marked as applied.
     * If the transformation is already partially applied, it will continue.
     *
     * @param transformation the transformation to be applied
     * @return a Mono that completes when the transformation has been applied.
     */
    Mono<Void> apply(Transformation transformation);
}
