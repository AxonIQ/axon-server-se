package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationApplyExecutor;
import reactor.core.publisher.Mono;

public class StandardTransformationApplyExecutor implements TransformationApplyExecutor {

    private final LocalTransformationApplyExecutor localTransformationApplyExecutor;

    public StandardTransformationApplyExecutor(LocalTransformationApplyExecutor localTransformationApplyExecutor) {
        this.localTransformationApplyExecutor = localTransformationApplyExecutor;
    }

    @Override
    public Mono<Void> apply(Transformation transformation) {
        return localTransformationApplyExecutor.apply(map(transformation))
                                               .then(transformation.markAsApplied());
    }

    private LocalTransformationApplyExecutor.Transformation map(Transformation transformation) {
        return new LocalTransformationApplyExecutor.Transformation() {
            @Override
            public String id() {
                return transformation.id();
            }

            @Override
            public String context() {
                return transformation.context();
            }

            @Override
            public int version() {
                return transformation.version();
            }

            @Override
            public long lastSequence() {
                return transformation.lastSequence();
            }
        };
    }
}
