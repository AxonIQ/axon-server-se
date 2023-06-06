/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.FileStoreEntry;
import org.springframework.data.util.CloseableIterator;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class MultiSegmentIterator implements CloseableIterator<FileStoreEntry> {

    private final Function<Long, CloseableIterator<FileStoreEntry>> iteratorProvider;
    private final AtomicLong nextIndex = new AtomicLong();
    private final Supplier<Long> lastIndexProvider;
    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile CloseableIterator<FileStoreEntry> iterator;

    public MultiSegmentIterator(Function<Long, CloseableIterator<FileStoreEntry>> iteratorProvider,
                                Supplier<Long> lastIndexProvider,
                                long nextIndex) {
        this.lastIndexProvider = lastIndexProvider;
        this.nextIndex.set(nextIndex);
        this.iteratorProvider = iteratorProvider;

        iterator = iteratorProvider.apply(nextIndex);
    }

    @Override
    public boolean hasNext() {
        if (closed.get()) {
            throw new IllegalStateException("Iterator already closed");
        }
        return nextIndex.get() <= lastIndexProvider.get();
    }

    @Override
    public FileStoreEntry next() {
        checkMoveToNextSegment();
        if (iterator == null || !hasNext()) {
            throw new NoSuchElementException(String.format("%d after %d", nextIndex.get(), lastIndexProvider.get()));
        }

        FileStoreEntry next = iterator.next();
        nextIndex.getAndIncrement();
        return next;
    }


    @Override
    public void close() {
        if (iterator != null) {
            closed.set(true);
            iterator.close();
        }
    }

    private void checkMoveToNextSegment() {
        if (iterator != null && iterator.hasNext()) {
            return;
        }

        iterator.close();
        iterator = iteratorProvider.apply(nextIndex.get());
    }
}
