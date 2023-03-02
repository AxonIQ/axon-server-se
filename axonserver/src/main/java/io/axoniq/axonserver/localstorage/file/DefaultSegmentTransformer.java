/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
                Optional<EventWithToken> eventWithToken = replacementSupplier.get();
                if (eventWithToken.isEmpty()) {
                    writeRemainingEvents();
                    sink.success();
                    return;
                }

                EventWithToken transformedEvent = eventWithToken.get();
                boolean found = false;
                do {
                    SerializedTransactionWithToken originalTX = currentOrNextOriginalTransaction();
                    if (originalTX == null) {
                        sink.success(); // or error as the segment does not contain the transaction we are looking for
                        return;
                    }

                    Event event = originalTX.getEvent(transformedTransaction.size());
                    if (transformedEvent.getToken() == originalTX.getToken() + transformedTransaction.size()) {
                        event = transform(event, transformedEvent.getEvent());
                        found = true;
                    }
                    transformedTransaction.add(event);

                    if (transformedTransaction.size() == originalTX.getEventsCount()) {
                        write(transformedTransaction);
                        transformedTransaction.clear();
                        originalTransactionRef.set(null);
                    }
                } while (!found);
                sink.success();
            } catch (Exception e) {
                sink.error(e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Event transform(Event originalEvent, Event replacementEvent) {
        if (Event.getDefaultInstance().equals(replacementEvent)) {
            return Event.newBuilder()
                        .setTimestamp(originalEvent.getTimestamp())
                        .setAggregateIdentifier(originalEvent.getAggregateIdentifier())
                        .setAggregateSequenceNumber(originalEvent.getAggregateSequenceNumber())
                        .setAggregateType(originalEvent.getAggregateType())
                        .setPayload(EMPTY_PAYLOAD)
                        .build();
        }
        return replacementEvent;
    }

    private SerializedTransactionWithToken currentOrNextOriginalTransaction() {
        if (originalTransactionRef.get() == null && transactionIteratorRef.get().hasNext()) {
            originalTransactionRef.set(transactionIteratorRef.get().next());
        }

        return originalTransactionRef.get();
    }

    private void write(List<Event> events) throws IOException {
        SegmentWriter segmentWriter = segmentWriterRef.get();
        segmentWriter.write(events);
    }

    private void writeRemainingEvents() throws IOException {
        // first complete the remaining events in the original transaction
        SerializedTransactionWithToken originalTX = originalTransactionRef.get();
        if (originalTX != null) {
            for (int i = transformedTransaction.size(); i < originalTX.getEventsCount(); i++) {
                transformedTransaction.add(originalTX.getEvent(transformedTransaction.size()));
            }
            write(transformedTransaction);
        }
        // write remaining transactions
        TransactionIterator transactionIterator = transactionIteratorRef.get();
        while (transactionIterator.hasNext()) {
            SerializedTransactionWithToken next = transactionIterator.next();
            write(next.getEvents().stream().map(SerializedEvent::asEvent).collect(Collectors.toList()));
        }
    }
}
