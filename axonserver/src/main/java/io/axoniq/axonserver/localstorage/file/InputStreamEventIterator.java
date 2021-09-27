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

import java.io.IOException;

/**
 * @author Marc Gathier
 */
public class InputStreamEventIterator extends EventIterator {

    private final InputStreamEventSource eventSource;
    private final PositionKeepingDataInputStream reader;

    public InputStreamEventIterator(InputStreamEventSource eventSource, long segment, long start) {
        reader = eventSource.getStream();
        this.eventSource = eventSource;
        currentSequenceNumber = segment;
        try {
            forwardTo(start);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
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
                short skip = (short) (firstSequence - currentSequenceNumber);
                readPartialTransaction(nrOfMessages, skip);
                reader.readInt();
            }
        }
    }

    private void readPartialTransaction(short nrOfMessages, short skip) throws IOException {
        for (short i = 0; i < skip; i++) {
            int messageSize = reader.readInt();
            reader.skipBytes(messageSize);
            currentSequenceNumber++;
        }
        for (short i = skip; i < nrOfMessages; i++) {
            addEvent();
        }
    }

    private void addEvent() throws IOException {
        int position = reader.position();
        eventsInTransaction.add(new EventInformation(position,
                                                     new SerializedEventWithToken(currentSequenceNumber, eventSource.readEvent())));
        currentSequenceNumber++;
    }

    protected boolean readTransaction() {
        try {
            int size = reader.readInt();
            if (size == -1 || size == 0) {
                return false;
            }
            processVersion(reader);
            short nrOfMessages = reader.readShort();
            for (int idx = 0; idx < nrOfMessages; idx++) {
                addEvent();
            }
            reader.readInt(); // checksum
            return true;
        } catch (IOException | RuntimeException exception) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                 "Failed to read event: " + currentSequenceNumber,
                                                 exception);
        }
    }

    @Override
    protected void doClose() {
        eventSource.close();
    }

    private void processVersion(PositionKeepingDataInputStream reader) throws IOException {
        reader.readByte();
    }


}
