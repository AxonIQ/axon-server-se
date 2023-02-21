package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.transformation.ActiveTransformation;
import io.axoniq.axonserver.eventstore.transformation.transformation.ApplyingTransformation;
import io.axoniq.axonserver.eventstore.transformation.transformation.FinalTransformation;
import reactor.core.publisher.Mono;

public class ContextTransformationStateConverter implements TransformationStateConverter {

    @Override
    public Mono<Transformation> from(TransformationState entity) {
        return Mono.fromSupplier(() -> {
            switch (entity.status()) {
                case ACTIVE:
                    return activeTransformation(entity);
                case APPLYING:
                    return applyingTransformation(entity);
                case APPLIED:
                case CANCELLED:
                case FAILED:
                    return finalTransformation();
                default:
                    throw new IllegalStateException("Unexpected transformation status.");
            }
        });
    }

    private ActiveTransformation activeTransformation(TransformationState state) {
        return new ActiveTransformation(state);
    }

    private  ApplyingTransformation applyingTransformation(TransformationState state) {
        return new ApplyingTransformation(state);
    }

    private FinalTransformation finalTransformation() {
        return new FinalTransformation();
    }

}
