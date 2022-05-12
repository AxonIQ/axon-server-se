package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationCancelExecutor;
import reactor.core.publisher.Mono;

public class StandardTransformationCancelExecutor implements TransformationCancelExecutor {

    private final LocalTransformationCancelExecutor localTransformationCancelExecutor;

    public StandardTransformationCancelExecutor(LocalTransformationCancelExecutor localTransformationCancelExecutor) {
        this.localTransformationCancelExecutor = localTransformationCancelExecutor;
    }

    @Override
    public Mono<Void> cancel(Transformation transformation) {
        return localTransformationCancelExecutor.cancel(map(transformation))
                                                .then(transformation.markAsCancelled());
    }

    private LocalTransformationCancelExecutor.Transformation map(Transformation transformation) {
        return new LocalTransformationCancelExecutor.Transformation() {
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
        };
    }
}
