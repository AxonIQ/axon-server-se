package io.axoniq.axonserver.eventstore.transformation.clean;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStoreProvider;
import reactor.core.publisher.Mono;

public class DefaultTransformationCleanExecutor implements TransformationCleanExecutor {

    private final TransformationEntryStoreProvider transformationEntryStoreSupplier;

    public DefaultTransformationCleanExecutor(TransformationEntryStoreProvider transformationEntryStoreSupplier) {
        this.transformationEntryStoreSupplier = transformationEntryStoreSupplier;
    }

    @Override
    public Mono<Void> clean(String context, String transformationId) {
        return transformationEntryStoreSupplier.provide(context, transformationId)
                                               .flatMap(TransformationEntryStore::delete);
    }

}
