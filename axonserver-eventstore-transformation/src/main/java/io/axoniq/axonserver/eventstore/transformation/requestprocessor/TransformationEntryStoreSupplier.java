package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface TransformationEntryStoreSupplier {

    Mono<TransformationEntryStore> supply(String context, String transformationId);
}
