/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author Marc Gathier
 */
public class InputStreamEventSource implements EventSource {
    private static final Logger logger = LoggerFactory.getLogger(InputStreamEventSource.class);
    private final PositionKeepingDataInputStream dataInputStream;
    private final EventTransformer eventTransformer;
    private volatile boolean closed;


    public InputStreamEventSource(File dataFile,
                                  EventTransformerFactory eventTransformerFactory,
                                  StorageProperties storageProperties) {
        try {
            logger.debug("Open file {}", dataFile);
            dataInputStream = new PositionKeepingDataInputStream(new BufferedInputStream(new FileInputStream(dataFile),
                                                                                         storageProperties
                                                                                                 .getReadBufferSize()));
            dataInputStream.readByte();
            int modifiers = dataInputStream.readInt();
            eventTransformer = eventTransformerFactory.get(modifiers);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public SerializedEvent readEvent(int position)  {
        try {
            dataInputStream.position(position);
            return readEvent();
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, ioException.getMessage(), ioException);
        }
    }

    public SerializedEvent readEvent() throws IOException {
        byte[] bytes = dataInputStream.readEvent();
        return new SerializedEvent(eventTransformer.fromStorage(bytes));
    }

    @Override
    public TransactionIterator createTransactionIterator(long segment, long token, boolean validating) {
        return new InputStreamTransactionIterator(this, segment, token, validating);
    }

    @Override
    public EventIterator createEventIterator(long segment, long startToken) {
        return new InputStreamEventIterator(this, segment, startToken);
    }

    public PositionKeepingDataInputStream getStream() {
        return dataInputStream;
    }

    @Override
    public void close()  {
        try {
            if( ! closed) {
                dataInputStream.close();
                closed = true;
            }
        } catch (IOException e) {
            logger.debug("Error while closing file", e);
        }
    }
}
