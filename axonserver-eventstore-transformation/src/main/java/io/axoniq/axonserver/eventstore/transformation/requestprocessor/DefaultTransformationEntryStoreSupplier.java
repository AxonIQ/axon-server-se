package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.filestorage.AppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.BaseAppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class DefaultTransformationEntryStoreSupplier implements TransformationEntryStoreSupplier {

    @FunctionalInterface
    public interface StoragePropertiesSupplier {

        StorageProperties storagePropertiesFor(String context);
    }

    private final StoragePropertiesSupplier storagePropertiesSupplier;
    private final Map<String, Mono<TransformationEntryStore>> cache = new ConcurrentHashMap<>();

    public DefaultTransformationEntryStoreSupplier(StoragePropertiesSupplier storagePropertiesSupplier) {
        this.storagePropertiesSupplier = storagePropertiesSupplier;
    }

    @Override
    public Mono<TransformationEntryStore> supply(String context) {
       return cache.computeIfAbsent(context, c -> Mono.fromSupplier(() -> autoOpen(() -> new BaseAppendOnlyFileStore(
                storagePropertiesSupplier.storagePropertiesFor(context), context))).cache());
    }

    private TransformationEntryStore autoOpen(Supplier<AppendOnlyFileStore> storeSupplier) {
        return new TransformationEntryStore() {

            private final AtomicReference<Mono<? extends TransformationEntryStore>> delegateRef = new AtomicReference<>();

            private Mono<? extends TransformationEntryStore> delegate() {
                delegateRef.compareAndSet(null, Mono.fromSupplier(storeSupplier)
                                                    .flatMap(fileStore -> fileStore.open(false).thenReturn(fileStore))
                                                    .map(SegmentBasedTransformationEntryStore::new)
                                                    .cacheInvalidateIf(SegmentBasedTransformationEntryStore::isClosed));
                return delegateRef.get();
            }

            @Override
            public Mono<Long> store(TransformationEntry entry) {
                return delegate().flatMap(d -> d.store(entry));
            }

            @Override
            public Flux<TransformationEntry> read() {
                return delegate().flatMapMany(TransformationEntryStore::read);
            }

            @Override
            public Mono<Void> delete() {
                return delegate().flatMap(TransformationEntryStore::delete);
            }
        };
    }
}