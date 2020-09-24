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
import io.axoniq.axonserver.exception.EventStoreValidationException;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Marc Gathier
 */
public class InputStreamTransactionIterator implements TransactionIterator {

    private final InputStreamEventSource eventSource;
    private final PositionKeepingDataInputStream reader;
    private long currentSequenceNumber;
    private final boolean validating;
    private SerializedTransactionWithToken next;

    public InputStreamTransactionIterator(InputStreamEventSource eventSource, long segment, long start, boolean validating) {
        this.eventSource = eventSource;
        this.reader = eventSource.getStream();
        this.currentSequenceNumber = segment;
        this.validating = validating;
        try {
            forwardTo(start);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
        readTransaction();
    }

    private void forwardTo(long firstSequence) throws IOException {
        while (firstSequence > currentSequenceNumber) {

            int size = reader.readInt();
            if (size == -1 || size == 0) {
                return;
            }
            processVersion(reader);
            short nrOfMessages = reader.readShort();

            if (firstSequence >= currentSequenceNumber + nrOfMessages) {
                reader.skipBytes(size + 4);
                currentSequenceNumber += nrOfMessages;
            } else {
                throw new EventStoreValidationException(
                        "FirstSequence in middle of transaction, firstSequence=" + firstSequence + ", current="
                                + currentSequenceNumber + ", nrOfMessages=" + nrOfMessages);
            }
        }
    }

    private byte processVersion(PositionKeepingDataInputStream reader) throws IOException {
        return reader.readByte();
    }

    private boolean readTransaction() {
        try {
            int size = reader.readInt();
            if (size == -1 || size == 0) {
                return false;
            }

            byte version = processVersion(reader);
            short nrOfMessages = reader.readShort();
            List<SerializedEvent> events = new ArrayList<>(nrOfMessages);
            for (int idx = 0; idx < nrOfMessages; idx++) {
                events.add(eventSource.readEvent());
            }
            next = new SerializedTransactionWithToken(currentSequenceNumber, version, events);
            currentSequenceNumber += nrOfMessages;
            reader.readInt(); // checksum
            return true;
        } catch (IOException | RuntimeException io) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, "Failed to read event: " + currentSequenceNumber, io);
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public SerializedTransactionWithToken next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        SerializedTransactionWithToken rv = next;
        if (!readTransaction()) {
            next = null;
            close();
        }
        return rv;
    }

    @Override
    public void close() {
        next = null;
        eventSource.close();
    }

}
