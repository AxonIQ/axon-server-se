package io.axoniq.axonserver.localstorage.transformation;

import reactor.core.publisher.Mono;

public interface LocalTransformationApplyExecutor {

    interface Transformation {

        String id();

        String context();

        int version();

        long lastSequence();
    }

    /**
     * This method is reentrant as anytime is invoked the apply restart from the latest applied sequence.
     * @param transformation
     * @return
     */
    Mono<Void> apply(Transformation transformation);

    Mono<Long> lastAppliedSequence(String transformationId);
}
