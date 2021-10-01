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
 * @author Marc Gathier
 * @since 4.6.0
 */
public class TransformationEntryStore {

    private final FileStore fileStore;
    private final AtomicBoolean open = new AtomicBoolean();

    public TransformationEntryStore(StorageProperties storageProperties, String id) {
        fileStore = new BaseFileStore(storageProperties, id);
    }

    public void open(boolean validate) {
        if (open.compareAndSet(false, true)) {
            fileStore.open(validate);
        }
    }

    public void delete() {
        fileStore.delete();
    }

    public CloseableIterator<TransformEventsRequest> iterator(int i) {
        CloseableIterator<FileStoreEntry> wrapped = fileStore.iterator(i);
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

     public Flux<TransformEventsRequest> entries() {
        return fileStore.stream(0).map(this::parse);
     }

    public Mono<Void> append(TransformEventsRequest replaceEventEntry) {
        return fileStore.append(new FileStoreEntry() {
            @Override
            public byte[] bytes() {
                return replaceEventEntry.toByteArray();
            }

            @Override
            public byte version() {
                return 0;
            }
        }).then();
    }

    public TransformEventsRequest lastEntry() {
        FileStoreEntry entry = fileStore.lastEntry();
        return parse(entry);
    }

    public TransformEventsRequest firstEntry() {
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
