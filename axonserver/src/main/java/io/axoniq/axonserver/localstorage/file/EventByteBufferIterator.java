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
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * @author Marc Gathier
 */
public class EventByteBufferIterator extends EventIterator {

    private final ByteBuffer reader;
    private final ByteBufferEventSource eventSource;


    public EventByteBufferIterator(ByteBufferEventSource eventSource, long segment, long token) {
        this.eventSource = eventSource;
        this.reader = eventSource.getBuffer();
        this.currentSequenceNumber = segment;
        forwardTo(token);
    }

    private void forwardTo(long firstSequence) {
            reader.position(5);
            while (firstSequence > currentSequenceNumber) {

                int size = reader.getInt();
                if (size == -1 || size == 0) {
                    return;
                }
                reader.get(); // version
                short nrOfMessages = reader.getShort();

                if (firstSequence >= currentSequenceNumber + nrOfMessages) {
                    reader.position(reader.position() + size + 4);
                    currentSequenceNumber += nrOfMessages;
                } else {
                    short skip = (short) (firstSequence - currentSequenceNumber);
                    readPartialTransaction(nrOfMessages, skip);
                    reader.getInt();
                }
            }

    }

    private void readPartialTransaction(short nrOfMessages, short skip) {
        for (short i = 0; i < skip; i++) {
            int messageSize = reader.getInt();
            reader.position(reader.position() + messageSize);
            currentSequenceNumber++;
        }
        for (short i = skip; i < nrOfMessages; i++) {
            addEvent();

        }
    }

    private void addEvent() {
        try {
            int position = reader.position();
            eventsInTransaction.add(new EventInformation(position, new SerializedEventWithToken(currentSequenceNumber, eventSource.readEvent())));
            currentSequenceNumber++;
        } catch( BufferUnderflowException io) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, "Failed to read event: " + currentSequenceNumber, io);
        }
    }

    protected boolean readTransaction() {
            int size = reader.getInt();
            if (size == -1 || size == 0) {
                reader.position(reader.position()-4);
                return false;
            }
            reader.get(); // version
            short nrOfMessages = reader.getShort();
            for( int idx = 0; idx < nrOfMessages ; idx++) {
                addEvent();
            }
            reader.getInt(); // checksum
            return true;
    }

    @Override
    protected void doClose() {
        eventSource.close();
    }
}
