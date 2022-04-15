package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface TransformationRollbackExecutor {

    interface Transformation {

        String id();

        int version();

        String context();
    }

    Mono<Void> rollback(Transformation transformation);
}
