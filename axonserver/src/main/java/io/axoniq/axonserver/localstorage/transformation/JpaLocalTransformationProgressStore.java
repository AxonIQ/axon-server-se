package io.axoniq.axonserver.localstorage.transformation;

import reactor.core.publisher.Mono;

public class JpaLocalTransformationProgressStore implements LocalTransformationProgressStore {

    @Override
    public Mono<TransformationApplyingState> stateFor(String transformationId) {
        return null;
    }

    @Override
    public Mono<Void> updateLastSequence(String transformationId, long lastProcessedSequence) {
        return null;
    }

    @Override
    public Mono<Void> markAsApplied(String transformationId) {
        return null;
    }
}
