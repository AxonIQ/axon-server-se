package io.axoniq.axonserver.eventstore.transformation.clean;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStoreSupplier;
import reactor.core.publisher.Mono;

public class DefaultTransformationCleanExecutor implements TransformationCleanExecutor {

    private final TransformationEntryStoreSupplier transformationEntryStoreSupplier;

    public DefaultTransformationCleanExecutor(TransformationEntryStoreSupplier transformationEntryStoreSupplier) {
        this.transformationEntryStoreSupplier = transformationEntryStoreSupplier;
    }

    @Override
    public Mono<Void> clean(String context, String transformationId) {
        return transformationEntryStoreSupplier.supply(context, transformationId)
                                               .flatMap(TransformationEntryStore::delete);
    }

}
