package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationApplier;
import reactor.core.publisher.Mono;

public class StandardTransformationApplier implements TransformationApplier {

    private final LocalTransformationApplier localTransformationApplier;

    public StandardTransformationApplier(LocalTransformationApplier localTransformationApplier) {
        this.localTransformationApplier = localTransformationApplier;
    }

    @Override
    public Mono<Void> apply(Transformation transformation) {
        return localTransformationApplier.apply(map(transformation))
                                         .then(transformation.markAsApplied());
    }

    private LocalTransformationApplier.Transformation map(Transformation transformation) {
        return new LocalTransformationApplier.Transformation() {
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
