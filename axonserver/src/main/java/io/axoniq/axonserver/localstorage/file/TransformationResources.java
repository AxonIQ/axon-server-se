package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

class TransformationResources {

    private final AtomicReference<SegmentTransformer> segmentTransformerRef =
            new AtomicReference<>(new NoopSegmentTransformer());

    private final Function<Long, Long> segmentForToken;
    private final Supplier<StorageProperties> storagePropertiesSupplier;
    private final int transformationVersion;
    private final IndexManager indexManager;
    private final Function<Long, TransactionIterator> transactionIteratorSupplier;
    private final String storagePath;

    TransformationResources(Function<Long, Long> segmentForToken,
                            Supplier<StorageProperties> storagePropertiesSupplier,
                            int transformationVersion,
                            IndexManager indexManager,
                            Function<Long, TransactionIterator> transactionIteratorSupplier,
                            String storagePath) {
        this.segmentForToken = segmentForToken;
        this.storagePropertiesSupplier = storagePropertiesSupplier;
        this.transformationVersion = transformationVersion;
        this.indexManager = indexManager;
        this.transactionIteratorSupplier = transactionIteratorSupplier;
        this.storagePath = storagePath;
    }


    public Mono<Void> transform(EventWithToken eventWithToken) {
        long segment = segmentForToken.apply(eventWithToken.getToken());
        if (segment > segmentTransformerRef.get().segment()) {
            DefaultSegmentTransformer segmentTransformer =
                    new DefaultSegmentTransformer(storagePropertiesSupplier.get(),
                                                  segment,
                                                  transformationVersion,
                                                  indexManager,
                                                  () -> transactionIteratorSupplier.apply(segment),
                                                  storagePath);
            SegmentTransformer prevSegmentTransformer = segmentTransformerRef.get();
            segmentTransformerRef.set(segmentTransformer);
            return prevSegmentTransformer.completeSegment()
                                         .then(segmentTransformer.initialize())
                                         .then(segmentTransformer.transformEvent(eventWithToken));
        }
        return segmentTransformerRef.get()
                                    .transformEvent(eventWithToken);
    }

    public Mono<Void> completeCurrentSegment() {
        return segmentTransformerRef.get()
                                    .completeSegment();
    }

    public Mono<Void> rollback(Throwable t) {
        return segmentTransformerRef.get()
                                    .rollback(t);
    }

    public Mono<Void> cancel() {
        return segmentTransformerRef.get()
                                    .cancel();
    }
}
