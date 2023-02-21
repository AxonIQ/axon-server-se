/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class DefaultSegmentTransformer implements SegmentTransformer {

    private final StorageProperties storageProperties;
    private final long segment;
    private final int newVersion;
    private final AtomicReference<TransactionIterator> transactionIteratorRef = new AtomicReference<>();
    private final AtomicReference<SegmentWriter> segmentWriterRef = new AtomicReference<>();
    private final AtomicReference<File> tempFileRef = new AtomicReference<>();
    private final AtomicReference<SerializedTransactionWithToken> originalTransactionRef = new AtomicReference<>();
    private final IndexManager indexManager;
    private final Supplier<TransactionIterator> transactionIteratorSupplier;
    private final String storagePath;
    private final List<Event> transformedTransaction = new CopyOnWriteArrayList<>();
    private static final SerializedObject EMPTY_PAYLOAD = SerializedObject.newBuilder().setType("empty").build();

    DefaultSegmentTransformer(StorageProperties storageProperties,
                              long segment,
                              int newVersion,
                              IndexManager indexManager,
                              Supplier<TransactionIterator> transactionIteratorSupplier,
                              String storagePath) {
        this.storageProperties = storageProperties;
        this.segment = segment;
        this.newVersion = newVersion;
        this.indexManager = indexManager;
        this.transactionIteratorSupplier = transactionIteratorSupplier;
        this.storagePath = storagePath;
    }

    @Override
    public Mono<Void> initialize() {
        return Mono.create(sink -> {
            try {
                File tempFile = storageProperties.transformedDataFile(storagePath,
                                                                      new FileVersion(segment, newVersion));
                FileUtils.delete(tempFile);
                tempFileRef.set(tempFile);
                SegmentWriter segmentWriter = new StreamSegmentWriter(tempFile,
                                                                      segment,
                                                                      storageProperties.getFlags());
                segmentWriterRef.set(segmentWriter);
                transactionIteratorRef.set(transactionIteratorSupplier.get());
                sink.success();
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    @Override
    public Mono<Void> transformEvent(EventWithToken transformedEvent) {
        return process(() -> Optional.of(transformedEvent));
    }

    @Override
    public Mono<Void> completeSegment() {
        return process(Optional::empty).then(
                Mono.create(sink -> {
                    try {
                        SegmentWriter segmentWriter = segmentWriterRef.get();
                        segmentWriter.writeEndOfFile();
                        segmentWriter.close();
                        indexManager.createNewVersion(segment, newVersion, segmentWriter.indexEntries());
                        closeTransactionIterator();
                        sink.success();
                    } catch (Exception e) {
                        sink.error(e);
                    }
                }));
    }

    @Override
    public Mono<Void> rollback(Throwable e) {
        return Mono.fromRunnable(() -> {
            closeTransactionIterator();
            File tempFile = tempFileRef.get();
            if (tempFile != null) {
                FileUtils.delete(tempFile);
            }
        });
    }

    @Override
    public Mono<Void> cancel() {
        closeTransactionIterator();
        return Mono.empty();
    }

    @Override
    public long segment() {
        return segment;
    }

    private void closeTransactionIterator() {
        TransactionIterator transactionIterator = transactionIteratorRef.getAndSet(null);
        if (transactionIterator != null) {
            transactionIterator.close();
        }
    }

    private Mono<Void> process(Supplier<Optional<EventWithToken>> replacementSupplier) {
        return Mono.<Void>create(sink -> {
            try {
                //todo to be simplified
                boolean done = false;
                do {
                    if (originalTransactionRef.get() == null) {
                        if (!transactionIteratorRef.get().hasNext()) {
                            sink.success();
                            return;
                        }
                        originalTransactionRef.set(transactionIteratorRef.get().next());
                    }

                    SerializedTransactionWithToken originalTX = originalTransactionRef.get();
                    Event event = originalTX.getEvents(transformedTransaction.size());


                    Optional<EventWithToken> eventWithToken = replacementSupplier.get();
                    if (eventWithToken.isPresent()) {
                        long currentToken = originalTX.getToken() + transformedTransaction.size();
                        EventWithToken transformedEvent = eventWithToken.get();
                        if (transformedEvent.getToken() == currentToken) {
                            Event replacement = transformedEvent.getEvent();
                            if (Event.getDefaultInstance().equals(replacement)) {
                                event = Event.newBuilder()
                                             .setTimestamp(event.getTimestamp())
                                             .setAggregateIdentifier(event.getAggregateIdentifier())
                                             .setAggregateSequenceNumber(event.getAggregateSequenceNumber())
                                             .setPayload(EMPTY_PAYLOAD)
                                             .build();
                            } else {
                                event = replacement;
                            }
                            done = true;
                        }
                    } else if (!transactionIteratorRef.get().hasNext()) {
                        done = true;
                    }
                    transformedTransaction.add(event);

                    if (originalTX.getEvents().size() == transformedTransaction.size()) {
                        SegmentWriter segmentWriter = segmentWriterRef.get();
                        segmentWriter.write(transformedTransaction);
                        originalTransactionRef.set(null);
                        transformedTransaction.clear();
                    }
                } while (!done);
                sink.success();
            } catch (Exception e) {
                sink.error(e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
