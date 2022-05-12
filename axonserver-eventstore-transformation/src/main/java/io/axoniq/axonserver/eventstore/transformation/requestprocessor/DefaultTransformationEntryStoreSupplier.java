package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.filestorage.impl.BaseAppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import reactor.core.publisher.Mono;

public class DefaultTransformationEntryStoreSupplier implements TransformationEntryStoreSupplier {

    @FunctionalInterface
    public interface StoragePropertiesSupplier {

        StorageProperties storagePropertiesFor(String context);
    }

    private final StoragePropertiesSupplier storagePropertiesSupplier;

    public DefaultTransformationEntryStoreSupplier(StoragePropertiesSupplier storagePropertiesSupplier) {
        this.storagePropertiesSupplier = storagePropertiesSupplier;
    }

    @Override
    public Mono<TransformationEntryStore> supply(String context) {
        return Mono.fromSupplier(() -> new BaseAppendOnlyFileStore(storagePropertiesSupplier.storagePropertiesFor(context),
                                                                   context))
                   .flatMap(fileStore -> fileStore.open(false).thenReturn(fileStore))
                   .map(SegmentBasedTransformationEntryStore::new);
    }
}