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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Marc Gathier
 */
public class TransactionByteBufferIterator implements TransactionIterator {

    private final ByteBuffer reader;
    private final ByteBufferEventSource eventSource;
    private long currentSequenceNumber;
    private final boolean validating;
    private SerializedTransactionWithToken next;


    public TransactionByteBufferIterator(ByteBufferEventSource eventSource, long segment, long token, boolean validating) {
        this.eventSource = eventSource;
        this.reader = eventSource.getBuffer();
        this.currentSequenceNumber = segment;
        this.validating = validating;
        forwardTo(token);
        readTransaction();
    }

    private void forwardTo(long firstSequence) {
        reader.position(5);
        while (firstSequence > currentSequenceNumber) {

            int size = reader.getInt();
            if (size == -1 || size == 0) {
                return;
            }
            reader.get();
            short nrOfMessages = reader.getShort();

            if (firstSequence >= currentSequenceNumber + nrOfMessages) {
                reader.position(reader.position() + size + 4);
                currentSequenceNumber += nrOfMessages;
            } else {
                throw new EventStoreValidationException(
                        "FirstSequence in middle of transaction, firstSequence=" + firstSequence + ", current="
                                + currentSequenceNumber + ", nrOfMessages=" + nrOfMessages);
            }
        }
    }

    private boolean readTransaction() {
        int size = reader.getInt();
        if (size == -1 || size == 0) {
            reader.position(reader.position()-4);
            return false;
        }
        byte version = reader.get();
        short nrOfMessages = reader.getShort();
        List<SerializedEvent> events = new ArrayList<>(nrOfMessages);
        int position = reader.position();
        for (int idx = 0; idx < nrOfMessages; idx++) {
            events.add(eventSource.readEvent());
        }
        next = new SerializedTransactionWithToken(currentSequenceNumber, version, events);
        currentSequenceNumber += nrOfMessages;
        int chk = reader.getInt(); // checksum
        if (validating) {
            Checksum checksum = new Checksum();
            checksum.update(reader, position, size);
            if( chk != checksum.get()) {
                throw new MessagingPlatformException(ErrorCode.VALIDATION_FAILED, "Invalid checksum at " + currentSequenceNumber);
            }
        }
        return true;
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
        }
        return rv;
    }

    @Override
    public void close() {
        next = null;
        eventSource.close();
    }
}
