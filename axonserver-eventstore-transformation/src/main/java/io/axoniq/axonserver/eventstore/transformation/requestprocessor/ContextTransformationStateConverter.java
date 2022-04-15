package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.active.ActiveTransformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.AppliedTransformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.ApplyingTransformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.FinalTransformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.RollingBackTransformation;
import reactor.core.publisher.Mono;

public class ContextTransformationStateConverter implements TransformationStateConverter {

    private final EventProvider eventProvider;

    public ContextTransformationStateConverter(EventProvider eventProvider) {
        this.eventProvider = eventProvider;
    }

    @Override
    public Mono<Transformation> from(TransformationState entity) {
        return Mono.fromSupplier(() -> {
            switch (entity.status()) {
                case ACTIVE:
                    return activeTransformation(entity);
                case APPLYING:
                    return applyingTransformation(entity);
                case APPLIED:
                    return appliedTransformation(entity);
                case ROLLING_BACK:
                    return rollingBackTransformation(entity);
                case ROLLED_BACK:
                case CANCELLED:
                case FAILED:
                    return finalTransformation();
                default:
                    throw new IllegalStateException("Unexpected transformation status.");
            }
        });
    }

    private ActiveTransformation activeTransformation(TransformationState state) {
        return new ActiveTransformation(resources(), state);
    }

    private  ApplyingTransformation applyingTransformation(TransformationState state) {
        return new ApplyingTransformation(state);
    }

    private  AppliedTransformation appliedTransformation(TransformationState state) {
        return new AppliedTransformation(state);
    }

    private  RollingBackTransformation rollingBackTransformation(TransformationState state) {
        return new RollingBackTransformation(state);
    }

    private FinalTransformation finalTransformation() {
        return new FinalTransformation();
    }

    private TransformationResources resources() {
        return new ContextTransformationResources(eventProvider);
    }
}
