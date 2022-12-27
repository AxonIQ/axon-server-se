package io.axoniq.axonserver.eventstore.transformation.apply;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStoreSupplier;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;


public class LocalMarkTransformationApplied implements MarkTransformationApplied {

    private static final Logger logger = LoggerFactory.getLogger(LocalMarkTransformationApplied.class);

    private final Transformers transformers;
    private final TransformationEntryStoreSupplier transformationEntryStoreSupplier;

    public LocalMarkTransformationApplied(Transformers transformers,
                                          TransformationEntryStoreSupplier transformationEntryStoreSupplier) {
        this.transformers = transformers;
        this.transformationEntryStoreSupplier = transformationEntryStoreSupplier;
    }

    @Override
    public Mono<Void> markApplied(String context, String transformationId) {
        return transformers.transformerFor(context)
                           .doOnNext(unused -> logger.warn("Marking as applied transformation {}", transformationId))
                           .flatMap(ct -> ct.markApplied(transformationId))
                           .then(deleteTransformationActions(context))
                           // TODO: 6/2/22 delete transformation actions
                           .doOnSuccess(unused -> logger.warn("Transformation {} marked applied.", transformationId));
    }

    private Mono<Void> deleteTransformationActions(String context) {
        return transformationEntryStoreSupplier.supply(context)
                                               .flatMap(TransformationEntryStore::delete);
    }
}
