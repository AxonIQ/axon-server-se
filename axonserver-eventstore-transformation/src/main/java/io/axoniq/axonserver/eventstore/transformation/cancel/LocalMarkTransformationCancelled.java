package io.axoniq.axonserver.eventstore.transformation.cancel;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class LocalMarkTransformationCancelled implements MarkTransformationCancelled {

    private static final Logger logger = LoggerFactory.getLogger(LocalMarkTransformationCancelled.class);

    private final Transformers transformers;

    public LocalMarkTransformationCancelled(Transformers transformers) {
        this.transformers = transformers;
    }

    @Override
    public Mono<Void> markCancelled(String context, String transformationId) {
        return transformers.transformerFor(context)
                           .doOnNext(unused -> logger.warn("Marking as cancelled transformation {}", transformationId))
                           .flatMap(ct -> ct.markCancelled(transformationId))
                           // TODO: 12/27/22 delete actions
                           .doOnSuccess(unused -> logger.warn("Transformation {} marked cancelled.", transformationId));
    }
}
