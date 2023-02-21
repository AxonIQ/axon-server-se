/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.FileStoreEntry;
import org.springframework.data.util.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * @author Marc Gathier
 */
public class ReaderEventIterator implements CloseableIterator<FileStoreEntry> {
    private final PositionKeepingReader reader;
    private volatile int nextSize;

    public ReaderEventIterator(File file, long segment,
                               long startIndex) {
        try {
            this.reader = new PositionKeepingReader(file);
            forwardTo(segment, startIndex);
        } catch (IOException e) {
            throw new FileStoreException(FileStoreErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    private void forwardTo(long segment, long firstSequence) throws IOException {
        long start = segment;
        reader.skipBytes(5);
        nextSize = reader.readInt();
        while (firstSequence >  start) {

            if (nextSize <= 0) {
                return;
            }

            reader.skipBytes(nextSize + 1 + 4);
            start++;
            nextSize = reader.readInt();
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        return nextSize > 0;
    }

    @Override
    public FileStoreEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            byte version = reader.readByte();
            byte[] bytes = reader.readBytes(nextSize);
            reader.skipBytes(4);
            nextSize = reader.readInt();
            return new FileStoreEntry() {
                @Override
                public byte[] bytes() {
                    return bytes;
                }

                @Override
                public byte version() {
                    return version;
                }
            };
        } catch (IOException e) {
            throw new FileStoreException(FileStoreErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }
}
