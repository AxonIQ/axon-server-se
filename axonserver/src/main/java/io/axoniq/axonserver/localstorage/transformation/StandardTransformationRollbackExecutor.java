package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationRollbackExecutor;
import reactor.core.publisher.Mono;

public class StandardTransformationRollbackExecutor implements TransformationRollbackExecutor {

    private final LocalTransformationRollbackExecutor localTransformationRollbackExecutor;

    public StandardTransformationRollbackExecutor(
            LocalTransformationRollbackExecutor localTransformationRollbackExecutor) {
        this.localTransformationRollbackExecutor = localTransformationRollbackExecutor;
    }

    @Override
    public Mono<Void> rollback(Transformation transformation) {
        return localTransformationRollbackExecutor.rollback(map(transformation))
                                                  .then(transformation.markRolledBack());
    }

    private LocalTransformationRollbackExecutor.Transformation map(Transformation transformation) {
        return new LocalTransformationRollbackExecutor.Transformation() {
            @Override
            public String id() {
                return transformation.id();
            }

            @Override
            public int version() {
                return transformation.version();
            }

            @Override
            public String context() {
                return transformation.context();
            }
        };
    }
}
