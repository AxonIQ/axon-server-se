package io.axoniq.axonserver.eventstore.transformation.clean;

import reactor.core.publisher.Mono;

public interface TransformationCleanExecutor {

    Mono<Void> clean(String context, String transformationId);
}
