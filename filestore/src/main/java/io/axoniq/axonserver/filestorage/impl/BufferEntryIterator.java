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

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * Iterator that iterates through log entries in a single log entry file.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public class BufferEntryIterator implements CloseableIterator<FileStoreEntry> {

    private final ByteBufferEntrySource source;

    /**
     * Creates the iterator.
     *  @param source    the buffer mapped to the file, current position is the position of the first log entry to read
     * @param segment
     * @param nextIndex index of the first log entry to read
     */
    public BufferEntryIterator(ByteBufferEntrySource source, long segment, long nextIndex) {
        this.source = source;
        for( long i = segment ; i < nextIndex; i++) {
            if( hasNext()) {
                next();
            } else {
                throw new IllegalArgumentException("Tried to initialize after last entry");
            }
        }
    }


    @Override
    public void close() {
    }

    @Override
    public boolean hasNext() {
        ByteBuffer buffer = source.getBuffer();
        int next = buffer.getInt(buffer.position());
        return next > 0;
    }

    @Override
    public FileStoreEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return source.readEntry();
    }
}
