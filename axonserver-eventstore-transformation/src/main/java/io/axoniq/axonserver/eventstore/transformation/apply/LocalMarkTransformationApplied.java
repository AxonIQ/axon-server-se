package io.axoniq.axonserver.eventstore.transformation.apply;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;


public class LocalMarkTransformationApplied implements MarkTransformationApplied {

    private static final Logger logger = LoggerFactory.getLogger(LocalMarkTransformationApplied.class);

    private final Transformers transformers;

    public LocalMarkTransformationApplied(Transformers transformers) {
        this.transformers = transformers;
    }

    @Override
    public Mono<Void> markApplied(String context, String transformationId) {
        return transformers.transformerFor(context)
                           .doOnNext(unused -> logger.warn("Marking as applied transformation {}", transformationId))
                           .flatMap(ct -> ct.markApplied(transformationId))
                           .doOnSuccess(unused -> logger.warn("Transformation {} marked applied.", transformationId));
    }
}
