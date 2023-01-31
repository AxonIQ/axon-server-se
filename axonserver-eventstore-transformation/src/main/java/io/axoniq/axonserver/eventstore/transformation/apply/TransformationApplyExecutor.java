package io.axoniq.axonserver.eventstore.transformation.apply;

import reactor.core.publisher.Mono;

public interface TransformationApplyExecutor {

    interface Transformation {

        String id();

        String context();

        int version();

        /**
         * Returns the last sequence contained in the transformation that needs to be applied.
         *
         * @return the last sequence contained in the transformation that needs to be applied.
         */
        long lastSequence();
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
