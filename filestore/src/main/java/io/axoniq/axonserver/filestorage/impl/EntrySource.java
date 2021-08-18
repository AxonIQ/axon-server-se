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

/**
 * Access to a file based log entry store.
 *
 * @author Marc Gathier
 * @Since 4.1
 */
public interface EntrySource extends AutoCloseable {

    int START_POSITION = 5;

    FileStoreEntry readEntry();

    default void close()  {
        // no-action
    }

    long segment();
    /**
     * Creates an iterator for a single segment of the log entry store.
     *
     * @param startIndex    the index of the first entry in the iterator
     * @return the iterator
     */
    CloseableIterator<FileStoreEntry> createLogEntryIterator(long startIndex);
}
