package io.axoniq.axonserver.eventstore.transformation.spi;

import reactor.core.publisher.Mono;

public interface TransformationAllowed {

    Mono<Void> validate(String context);
}
