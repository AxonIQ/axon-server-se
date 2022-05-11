package io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import reactor.core.publisher.Mono;

public class CancellingTransformation implements Transformation {

    private final TransformationState state;

    public CancellingTransformation(TransformationState state) {
        this.state = state;
    }

    @Override
    public Mono<TransformationState> markCancelled() {
        return Mono.fromSupplier(() -> state.withStatus(EventStoreTransformationJpa.Status.CANCELLED));
    }
}
