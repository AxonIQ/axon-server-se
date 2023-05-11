/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
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

import java.io.File;
import java.io.IOException;

/**
 * @author Marc Gathier
 */
public class InputStreamEventSource implements EventSource {

    private static final Logger logger = LoggerFactory.getLogger(InputStreamEventSource.class);
    private final File dataFile;
    private final long segment;
    private final EventTransformerFactory eventTransformerFactory;
    private volatile boolean closed;


    public InputStreamEventSource(File dataFile,
                                  long segment,
                                  EventTransformerFactory eventTransformerFactory) {
        this.dataFile = dataFile;
        this.segment = segment;

        this.eventTransformerFactory = eventTransformerFactory;
    }


    @Override
    public Reader reader() {
        return new DataInputStreamReader();
    }

    @Override
    public TransactionIterator createTransactionIterator(long token, boolean validating) {
        return new InputStreamTransactionIterator(dataFile, eventTransformerFactory, segment, token);
    }

    @Override
    public EventIterator createEventIterator(long startToken) {
        return new InputStreamEventIterator(dataFile, eventTransformerFactory, segment, startToken);
    }

    @Override
    public long segment() {
        return segment;
    }

    public PositionKeepingDataInputStream getStream() throws IOException {
        return new PositionKeepingDataInputStream(dataFile);
    }


    private class DataInputStreamReader implements Reader {

        private final PositionKeepingDataInputStream dataInputStream;
        private final EventTransformer eventTransformer;

        private DataInputStreamReader() {
            try {
                logger.debug("Open file {}", dataFile);
                dataInputStream = new PositionKeepingDataInputStream(dataFile);
                this.eventTransformer = eventTransformerFactory.get(dataInputStream.flags());
            } catch (IOException e) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
            }
        }

        @Override
        public SerializedEvent readEvent(int position) {
            try {
                dataInputStream.position(position);
                byte[] bytes = dataInputStream.readEvent();
                return new SerializedEvent(eventTransformer.fromStorage(bytes));
            } catch (IOException ioException) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     ioException.getMessage(),
                                                     ioException);
            }
        }

        @Override
        public void close() {
            try {
                logger.debug("Close file {} - {}", dataFile, closed);
                if (!closed) {
                    dataInputStream.close();
                    closed = true;
                }
            } catch (IOException e) {
                logger.debug("Error while closing file", e);
            }
        }
    }
}
