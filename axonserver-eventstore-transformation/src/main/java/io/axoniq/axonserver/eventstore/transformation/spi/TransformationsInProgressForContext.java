package io.axoniq.axonserver.eventstore.transformation.spi;

import reactor.core.publisher.Mono;

public interface TransformationsInProgressForContext {

    Mono<Boolean> inProgress(String context);
}
