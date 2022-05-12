package io.axoniq.axonserver.localstorage.transformation;

import reactor.core.publisher.Mono;

public interface LocalTransformationRollbackExecutor {

    interface Transformation {

        String id();

        int version();

        String context();
    }

    Mono<Void> rollback(Transformation transformation);
}
