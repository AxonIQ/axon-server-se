package io.axoniq.axonserver.localstorage.transformation;

import reactor.core.publisher.Mono;

public interface LocalTransformationProgressStore {

    /**
     * It will never return an empty Mono, because if this is empty the default initial state is returned.
     *
     * @param transformationId the identifier of the transformation
     * @return
     */
    Mono<TransformationApplyingState> stateFor(String transformationId);

    Mono<Void> updateLastSequence(String transformationId, long lastProcessedSequence);

    Mono<Void> markAsApplied(String transformationId);
}
