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
import io.axoniq.axonserver.exception.EventStoreValidationException;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Segment of the event store accessed by a memory mapped file.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class ByteBufferEventSource implements EventSource {

    private static final Logger logger = LoggerFactory.getLogger(ByteBufferEventSource.class);

    private final EventTransformer eventTransformer;
    protected final ByteBuffer buffer;
    protected final AtomicInteger duplicatesCount = new AtomicInteger();
    private final String path;
    // indicates if the low-level clean method should be called (needed to free file lock on windows)
    private final boolean cleanerHack;
    private final int version;
    private final long segment;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ByteBufferEventSource(String path, ByteBuffer buffer,
                                 long segment, int version,
                                 EventTransformerFactory eventTransformerFactory,
                                 StorageProperties storageProperties) {
        this.path = path;
        this.segment = segment;
        this.version = version;
        buffer.get();
        int flags = buffer.getInt();
        this.eventTransformer = eventTransformerFactory.get(flags);
        this.buffer = buffer;
        this.cleanerHack = storageProperties.isCleanRequired();
    }

    @Override
    public Reader reader() {
        duplicatesCount.incrementAndGet();
        try {
            checkClosed();
            return new ByteBufferReader();
        } catch (IllegalStateException illegalStateException) {
            duplicatesCount.decrementAndGet();
            throw illegalStateException;
        }
    }


    public int version() {
        return version;
    }


    @Override
    public TransactionIterator createTransactionIterator(long token, boolean validating) {
        duplicatesCount.incrementAndGet();
        try {
            checkClosed();
            return new TransactionByteBufferIterator(token, validating);
        } catch (IllegalStateException illegalStateException) {
            duplicatesCount.decrementAndGet();
            throw illegalStateException;
        }
    }

    @Override
    public EventIterator createEventIterator(long startToken) {
        duplicatesCount.incrementAndGet();
        try {
            checkClosed();
            return new EventByteBufferIterator(startToken);
        } catch (IllegalStateException illegalStateException) {
            duplicatesCount.decrementAndGet();
            throw illegalStateException;
        }
    }

    @Override
    public long segment() {
        return segment;
    }


    public void clean(long delay) {
        logger.trace("{}: Clean after {} seconds - {} {}", path, delay, cleanerHack, duplicatesCount);
        if (closed.compareAndSet(false, true) && cleanerHack) {
            CleanUtils.cleanDirectBuffer(buffer, () -> duplicatesCount.get() == 0, delay, path);
        }
    }

    protected void checkClosed() {
        if (closed.get()) {
            throw new IllegalStateException("File already closed");
        }
    }

    private SerializedEvent doReadEvent(ByteBuffer reader) {
        int size = reader.getInt();
        byte[] bytes = new byte[size];
        reader.get(bytes);
        return new SerializedEvent(eventTransformer.fromStorage(bytes));
    }


    @SuppressWarnings("deprecation")
    @Override
    protected void finalize() throws Throwable {
        clean(0);
    }

    private class ByteBufferReader implements Reader {

        private final ByteBuffer reader;

        private ByteBufferReader() {
            this.reader = buffer.duplicate();
        }

        @Override
        public SerializedEvent readEvent(int position) {
            duplicatesCount.incrementAndGet();
            try {
                checkClosed();
                reader.position(position);
                return doReadEvent(reader);
            } catch (OutOfMemoryError oom) {
                throw new RuntimeException(oom);
            } finally {
                duplicatesCount.decrementAndGet();
            }
        }


        @Override
        public void close() {
            duplicatesCount.decrementAndGet();
        }
    }

    private class TransactionByteBufferIterator implements TransactionIterator {

        private final ByteBuffer reader;
        private long currentSequenceNumber;
        private final boolean validating;
        private SerializedTransactionWithToken next;


        public TransactionByteBufferIterator(long token, boolean validating) {
            this.reader = buffer.duplicate();
            this.currentSequenceNumber = segment();
            this.validating = validating;
            forwardTo(token);
            readTransaction();
            logger.debug("Open transaction iterator {} duplicates = {}", path, duplicatesCount);
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
                reader.position(reader.position() - 4);
                return false;
            }
            byte eventFormatVersion = reader.get();
            short nrOfMessages = reader.getShort();
            List<SerializedEvent> events = new ArrayList<>(nrOfMessages);
            int position = reader.position();
            for (int idx = 0; idx < nrOfMessages; idx++) {
                events.add(doReadEvent(reader));
            }
            next = new SerializedTransactionWithToken(currentSequenceNumber, eventFormatVersion, events);
            currentSequenceNumber += nrOfMessages;
            int chk = reader.getInt(); // checksum
            if (validating) {
                Checksum checksum = new Checksum();
                checksum.update(reader, position, size);
                if (chk != checksum.get()) {
                    throw new MessagingPlatformException(ErrorCode.VALIDATION_FAILED,
                                                         "Invalid checksum at " + currentSequenceNumber);
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
            duplicatesCount.decrementAndGet();
            logger.debug("Close transaction iterator {} duplicates = {}", path, duplicatesCount);
        }
    }

    private class EventByteBufferIterator extends EventIterator {

        private final ByteBuffer reader;

        public EventByteBufferIterator(long token) {
            this.reader = buffer.duplicate();
            this.currentSequenceNumber = segment();
            forwardTo(token);
            logger.debug("Open event iterator {} duplicates = {}", path, duplicatesCount);
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
                eventsInTransaction.add(new EventInformation(position,
                                                             new SerializedEventWithToken(currentSequenceNumber,
                                                                                          doReadEvent(reader))));
                currentSequenceNumber++;
            } catch (BufferUnderflowException io) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     "Failed to read event: " + currentSequenceNumber,
                                                     io);
            }
        }

        protected boolean readTransaction() {
            int size = reader.getInt();
            if (size == -1 || size == 0) {
                reader.position(reader.position() - 4);
                return false;
            }
            reader.get(); // version
            short nrOfMessages = reader.getShort();
            for (int idx = 0; idx < nrOfMessages; idx++) {
                addEvent();
            }
            reader.getInt(); // checksum
            return true;
        }

        @Override
        public int position() {
            return reader.position();
        }

        @Override
        protected void doClose() {
            duplicatesCount.decrementAndGet();
            logger.debug("Close event iterator {} duplicates = {}", path, duplicatesCount);
        }
    }
}
