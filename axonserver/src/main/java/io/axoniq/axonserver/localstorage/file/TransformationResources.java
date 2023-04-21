/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

class TransformationResources {

    private final AtomicReference<SegmentTransformer> segmentTransformerRef =
            new AtomicReference<>(new NoopSegmentTransformer());

    private final AtomicLong count = new AtomicLong();

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

//    public Flux<Long> transform(Flux<EventWithToken> events){
//        return Flux.create(fluxSink -> {
//            events.concatMap(event -> transform(event, fluxSink))
//                    .then(completeCurrentSegment())
//                    .doOnSuccess(s -> fluxSink.next(count.getAndSet(0L)))
//                    .doOnError(error -> fluxSink.error(error))
//                    .subscribe();
//        });
//    }
//
//    public Mono<Void> transform(EventWithToken eventWithToken, FluxSink<Long> fluxSink) {
//        long segment = segmentForToken.apply(eventWithToken.getToken());
//        if (segment > segmentTransformerRef.get().segment()) {
//            DefaultSegmentTransformer segmentTransformer =
//                    new DefaultSegmentTransformer(storagePropertiesSupplier.get(),
//                                                  segment,
//                                                  transformationVersion,
//                                                  indexManager,
//                                                  () -> transactionIteratorSupplier.apply(segment),
//                                                  storagePath);
//            SegmentTransformer prevSegmentTransformer = segmentTransformerRef.getAndSet(segmentTransformer);
//            return prevSegmentTransformer.completeSegment()
//                                         .then(segmentTransformer.initialize())
//                                         .then(segmentTransformer.transformEvent(eventWithToken))
//                    .doOnSuccess(v -> fluxSink.next(count.getAndSet(0L)))
//                                         .doOnError(error -> error.printStackTrace());
//        }
//        return segmentTransformerRef.get()
//                                    .transformEvent(eventWithToken)
//                                    .doOnSuccess(v -> count.incrementAndGet());
//    }

    public Mono<Long> transform(EventWithToken eventWithToken) {
        long segment = segmentForToken.apply(eventWithToken.getToken());
        if (segment > segmentTransformerRef.get().segment()) {
            DefaultSegmentTransformer segmentTransformer =
                    new DefaultSegmentTransformer(storagePropertiesSupplier.get(),
                                                  segment,
                                                  transformationVersion,
                                                  indexManager,
                                                  () -> transactionIteratorSupplier.apply(segment),
                                                  storagePath);
            SegmentTransformer prevSegmentTransformer = segmentTransformerRef.getAndSet(segmentTransformer);
            return prevSegmentTransformer.completeSegment()
                                         .then(segmentTransformer.initialize())
                                         .then(segmentTransformer.transformEvent(eventWithToken))
                                         .thenReturn(count.getAndSet(0L))
                                         .doOnError(error -> error.printStackTrace());
        }
        return segmentTransformerRef.get()
                                    .transformEvent(eventWithToken)
                                    .doOnSuccess(v -> count.incrementAndGet())
                                    .thenReturn(0L);
    }

    public Mono<Long> completeCurrentSegment() {
        return Mono.defer(() ->
                                  segmentTransformerRef.get()
                                                       .completeSegment()
                                                       .thenReturn(count.getAndSet(0L))
        );
    }

    public Mono<Void> rollback(Throwable t) {
        return Mono.defer(() -> segmentTransformerRef.get()
                                                     .rollback(t)
                                                     .doOnSubscribe(s -> System.out.println("Rollback invoked")));
    }

    public Mono<Void> cancel() {
        return Mono.defer(() ->
                                  segmentTransformerRef.get()
                                                       .cancel());
    }
}
