package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface TransformationCancelExecutor {

    interface Transformation {

        String id();

        String context();

        int version();

        Mono<Void> markAsCancelled();
    }

    Mono<Void> cancel(Transformation transformation);
}
