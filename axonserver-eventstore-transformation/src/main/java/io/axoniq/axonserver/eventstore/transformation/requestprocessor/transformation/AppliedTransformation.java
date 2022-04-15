package io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import reactor.core.publisher.Mono;

import static io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa.Status.ROLLING_BACK;

public class AppliedTransformation implements Transformation {

    private final TransformationState state;

    public AppliedTransformation(TransformationState state) {
        this.state = state;
    }

    @Override
    public Mono<TransformationState> startRollingBack() {
        return Mono.fromSupplier(() -> state.withStatus(ROLLING_BACK));
    }
}
