package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.filestorage.AppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.BaseAppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class DefaultTransformationEntryStoreSupplier implements TransformationEntryStoreProvider {

    @FunctionalInterface
    public interface StoragePropertiesSupplier {

        StorageProperties storagePropertiesFor(String context, String transformationId);
    }

    private final StoragePropertiesSupplier storagePropertiesSupplier;
    private final Map<TransformationId, Mono<TransformationEntryStore>> cache = new ConcurrentHashMap<>();

    public DefaultTransformationEntryStoreSupplier(StoragePropertiesSupplier storagePropertiesSupplier) {
        this.storagePropertiesSupplier = storagePropertiesSupplier;
    }

    @Override
    public Mono<TransformationEntryStore> provide(String context, String transformationId) {
        return cache.computeIfAbsent(new TransformationId(context, transformationId),
                                     id -> Mono.fromSupplier(() -> autoOpen(() -> new BaseAppendOnlyFileStore(
                                             storagePropertiesSupplier.storagePropertiesFor(context, transformationId),
                                             context))).cache());
    }

    // TODO: 8/12/22 extract to a separate class
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

    private static class TransformationId {
        private final String context;
        private final String id;

        private TransformationId(String context, String id) {
            this.context = context;
            this.id = id;
        }

        public String context() {
            return context;
        }

        public String id() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TransformationId that = (TransformationId) o;
            return context.equals(that.context) && id.equals(that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(context, id);
        }
    }
}