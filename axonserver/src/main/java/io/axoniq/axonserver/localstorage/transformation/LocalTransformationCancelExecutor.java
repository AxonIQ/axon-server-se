package io.axoniq.axonserver.localstorage.transformation;

import reactor.core.publisher.Mono;

public interface LocalTransformationCancelExecutor {

    interface Transformation {

        String id();

        String context();

        int version();
    }

    Mono<Void> cancel(Transformation transformation);
}
