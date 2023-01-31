package io.axoniq.axonserver.eventstore.transformation.apply;

import reactor.core.publisher.Mono;

public interface MarkTransformationApplied {

    Mono<Void> markApplied(String context, String transformationId);
}
