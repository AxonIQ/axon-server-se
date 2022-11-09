package io.axoniq.axonserver.eventstore.transformation.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import reactor.core.publisher.Mono;

public class ApplyingTransformation implements Transformation {

    private final TransformationState state;

    public ApplyingTransformation(TransformationState state) {
        this.state = state;
    }

    @Override
    public Mono<TransformationState> markApplied() {
        return Mono.fromSupplier(state::applied);
    }
}
