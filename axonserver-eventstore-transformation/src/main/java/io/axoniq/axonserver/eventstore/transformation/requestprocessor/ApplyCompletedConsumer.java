package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface ApplyCompletedConsumer {

    Mono<Void> onLocallyApplied(String context, String transformationId);
}