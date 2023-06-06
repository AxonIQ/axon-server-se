package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface TransformationEntryStoreProvider {

    Mono<TransformationEntryStore> provide(String context, String transformationId);
}
