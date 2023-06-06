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
                           .doFirst(() -> logger.info("Marking as applied transformation {}", transformationId))
                           .flatMap(ct -> ct.markApplied(transformationId))
                           .doOnSuccess(unused -> logger.info("Transformation {} marked applied.", transformationId))
                           .doOnError(error -> logger.warn("Error marking the transformation {} as applied.",
                                                           transformationId,
                                                           error));
    }
}
