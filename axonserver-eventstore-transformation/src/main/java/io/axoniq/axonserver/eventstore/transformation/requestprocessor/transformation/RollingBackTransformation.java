package io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import reactor.core.publisher.Mono;

import static io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa.Status.ROLLED_BACK;

public class RollingBackTransformation implements Transformation {

    private final TransformationState state;

    public RollingBackTransformation(TransformationState state) {
        this.state = state;
    }

    @Override
    public Mono<TransformationState> markRolledBack() {
        return Mono.fromSupplier(() -> state.withStatus(ROLLED_BACK));
    }

}
