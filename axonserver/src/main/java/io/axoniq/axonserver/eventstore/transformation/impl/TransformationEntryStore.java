/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.filestorage.FileStore;
import io.axoniq.axonserver.filestorage.FileStoreEntry;
import io.axoniq.axonserver.filestorage.impl.BaseFileStore;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * File store containing transformation actions for a specific transformation on a context.
 * @author Marc Gathier
 * @since 4.6.0
 */
public class TransformationEntryStore {

    private final FileStore fileStore;
    private final AtomicBoolean open = new AtomicBoolean();

    /**
     * Constructor for the transformation entry store.
     * @param storageProperties  configuration of the store
     * @param id    store identifier
     */
    public TransformationEntryStore(StorageProperties storageProperties, String id) {
        fileStore = new BaseFileStore(storageProperties, id);
    }

    /**
     * Opens a file store for transformation entries. If the file store does not exist it will be created.
     * @param validate perform validation of the existing store
     */
    public void open(boolean validate) {
        if (open.compareAndSet(false, true)) {
            fileStore.open(validate);
        }
    }

    /**
     * Delete a file store.
     */
    public void delete() {
        fileStore.delete();
    }

    /**
     * Create an iterator for the entry store from a specific index.
     *
     * @param start the index in the file store where to start
     * @return an iterator of entries
     */
    public CloseableIterator<TransformEventsRequest> iterator(int start) {
        CloseableIterator<FileStoreEntry> wrapped = fileStore.iterator(start);
        return new CloseableIterator<TransformEventsRequest>() {
            @Override
            public void close() {
                wrapped.close();
            }

            @Override
            public boolean hasNext() {
                return wrapped.hasNext();
            }

            @Override
            public TransformEventsRequest next() {
                return parse(wrapped.next());
            }
        };
    }

    /**
     * Returns a flux with all entries in the entry store.
     * @return a flux of entries
     */
     public Flux<TransformEventsRequest> entries() {
        return fileStore.stream(0).map(this::parse);
     }

    /**
     * Adds an entry to the entry store.
     * @param request the entry to add
     * @return a mono that is completed when the entry is stored.
     */
    public Mono<Void> append(TransformEventsRequest request) {
        return fileStore.append(new FileStoreEntry() {
            @Override
            public byte[] bytes() {
                return request.toByteArray();
            }

            @Override
            public byte version() {
                return 0;
            }
        }).then();
    }

    /**
     * Returns the last entry in the entry store. Returns null is store is empty.
     * @return an entry or null
     */
    public TransformEventsRequest lastEntry() {
        FileStoreEntry entry = fileStore.lastEntry();
        return entry == null ? null : parse(entry);
    }

    /**
     * Returns the first entry in the entry store. Returns null is store is empty.
     * @return an entry or null
     */
    public TransformEventsRequest firstEntry() {
        if (fileStore.isEmpty()) return null;
        FileStoreEntry entry = fileStore.read(0).block();
        return parse(entry);
    }

    private TransformEventsRequest parse(FileStoreEntry entry) {
        if (entry == null) {
            return null;
        }
        try {
            return TransformEventsRequest.parseFrom(entry.bytes());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
