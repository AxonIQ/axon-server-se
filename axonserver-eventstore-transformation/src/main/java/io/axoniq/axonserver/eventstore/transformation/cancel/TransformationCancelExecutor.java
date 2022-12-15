package io.axoniq.axonserver.eventstore.transformation.cancel;

import reactor.core.publisher.Mono;

public interface TransformationCancelExecutor {

    interface Transformation {

        String id();

        String context();

        int version();

    }

    Mono<Void> cancel(Transformation transformation);
}
