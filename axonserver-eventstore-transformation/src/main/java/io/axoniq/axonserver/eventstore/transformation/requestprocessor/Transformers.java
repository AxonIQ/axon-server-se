package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

public interface Transformers {

    Mono<ContextTransformer> transformerFor(String context);

    Mono<Void> clean(String context);
}
