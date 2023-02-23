package io.axoniq.axonserver.eventstore.transformation.apply;

import reactor.core.publisher.Mono;

public interface TransformationProgressStore {

    Mono<TransformationApplyingState> initState(String transformationId);

    /**
     * It will never return an empty Mono, because if this is empty the default initial state is returned.
     *
     * @param transformationId the identifier of the transformation
     * @return
     */
    Mono<TransformationApplyingState> stateFor(String transformationId);

    Mono<Void> incrementLastSequence(String transformationId, long sequenceIncrement);

    Mono<Void> markAsApplied(String transformationId);

    Mono<Void> clean();
}
