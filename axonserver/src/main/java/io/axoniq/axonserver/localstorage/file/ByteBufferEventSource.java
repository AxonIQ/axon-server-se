/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Segment of the event store accessed by a memory mapped file.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class ByteBufferEventSource implements EventSource {

    private final EventTransformer eventTransformer;
    private final Runnable onClose;
    private final ByteBuffer buffer;
    private final boolean main;
    private final AtomicInteger duplicatesCount = new AtomicInteger();
    private final String path;
    // indicates if the low-level clean method should be called (needed to free file lock on windows)
    private final boolean cleanerHack;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final int version;
    private final long segment;

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
        this.main = true;
        this.onClose = null;
        this.cleanerHack = storageProperties.isCleanRequired();
    }

    protected ByteBufferEventSource(String path, ByteBuffer buffer,
                                    long segment, int version,
                                    EventTransformer eventTransformer,
                                    boolean cleanerHack, Runnable onClose) {
        this.path = path;
        this.segment = segment;
        this.version = version;
        this.buffer = buffer;
        this.eventTransformer = eventTransformer;
        this.onClose = onClose;
        this.main = false;
        this.cleanerHack = cleanerHack;
    }

    protected ByteBufferEventSource(String path, ByteBuffer buffer,
                                    long segment, int version,
                                    EventTransformer eventTransformer,
                                    boolean cleanerHack) {
        this.path = path;
        this.segment = segment;
        this.version = version;
        this.buffer = buffer;
        this.eventTransformer = eventTransformer;
        this.onClose = null;
        this.main = true;
        this.cleanerHack = cleanerHack;
    }

    public SerializedEvent readEvent() {
        int size = buffer.getInt();
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new SerializedEvent(eventTransformer.fromStorage(bytes));
    }

    public ByteBufferEventSource duplicate() {
        duplicatesCount.incrementAndGet();
        return new ByteBufferEventSource(path,
                                         buffer.duplicate(),
                                         segment,
                                         version,
                                         eventTransformer,
                                         cleanerHack,
                                         duplicatesCount::decrementAndGet);
    }

    public int version() {
        return version;
    }

    @Override
    protected void finalize() {
        if (cleanerHack && main) {
            CleanUtils.cleanDirectBuffer(buffer, () -> duplicatesCount.get() == 0, 60, path);
        }
    }

    public SerializedEvent readEvent(int position) {
        try {
            buffer.position(position);
            return readEvent();
        } catch (OutOfMemoryError oom) {
            throw new RuntimeException(oom);
        }
    }

    @Override
    public TransactionIterator createTransactionIterator(long token, boolean validating) {
        return new TransactionByteBufferIterator(this, token, validating);
    }

    @Override
    public EventIterator createEventIterator(long startToken) {
        return new EventByteBufferIterator(this, startToken);
    }

    @Override
    public long segment() {
        return segment;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int position() {
        return buffer.position();
    }

    public void clean(long delay) {
        close();
        if (cleanerHack && main) {
            CleanUtils.cleanDirectBuffer(getBuffer(), () -> duplicatesCount.get() == 0, delay, path);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) && onClose != null) {
            onClose.run();
        }
    }
}
