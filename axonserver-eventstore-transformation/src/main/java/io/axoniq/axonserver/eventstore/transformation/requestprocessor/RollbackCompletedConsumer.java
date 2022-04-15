package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface RollbackCompletedConsumer {

    Mono<Void> onLocallyRolledBack(String context, String transformationId);
}
