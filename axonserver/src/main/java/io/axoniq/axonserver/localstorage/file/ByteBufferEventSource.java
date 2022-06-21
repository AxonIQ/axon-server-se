/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;

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

    private final Runnable onClose;
    private final ByteBuffer buffer;
    private final boolean main;
    private final AtomicInteger duplicatesCount = new AtomicInteger();
    private final String path;
    // indicates if the low-level clean method should be called (needed to free file lock on windows)
    private final boolean cleanerHack;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ByteBufferEventSource(String path, ByteBuffer buffer, StorageProperties storageProperties) {
        this.path = path;
        buffer.get();
        this.buffer = buffer;
        this.main = true;
        this.onClose = null;
        this.cleanerHack = storageProperties.isCleanRequired();
    }

    protected ByteBufferEventSource(String path, ByteBuffer buffer, boolean cleanerHack, Runnable onClose) {
        this.path = path;
        this.buffer = buffer;
        this.onClose = onClose;
        this.main = false;
        this.cleanerHack = cleanerHack;
    }

    protected ByteBufferEventSource(String path, ByteBuffer buffer, boolean cleanerHack) {
        this.path = path;
        this.buffer = buffer;
        this.onClose = null;
        this.main = true;
        this.cleanerHack = cleanerHack;
    }

    public SerializedEvent readEvent() {
        int size = buffer.getInt();
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new SerializedEvent(bytes);
    }

    public ByteBufferEventSource duplicate() {
        duplicatesCount.incrementAndGet();
        return new ByteBufferEventSource(path,
                                         buffer.duplicate(),
                                         cleanerHack,
                                         duplicatesCount::decrementAndGet);
    }

    @Override
    protected void finalize() {
        if (cleanerHack && main) {
            CleanUtils.cleanDirectBuffer(buffer, () -> duplicatesCount.get() == 0, 60, path);
        }
    }

    public SerializedEvent readEvent(int position) {
        buffer.position(position);
        return readEvent();
    }

    @Override
    public TransactionIterator createTransactionIterator(long segment, long token, boolean validating) {
        return new TransactionByteBufferIterator(this, segment, token, validating);
    }

    @Override
    public EventIterator createEventIterator(long segment, long startToken) {
        return new EventByteBufferIterator(this, segment, startToken);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int position() {
        return buffer.position();
    }

    public void clean(long delay) {
        if (cleanerHack && main) {
            CleanUtils.cleanDirectBuffer(getBuffer(), () -> duplicatesCount.get() == 0, delay, path);
        }
    }

    @Override
    public void close() {
        if( onClose != null) {
            if (closed.compareAndSet(false, true))  {
                onClose.run();
            }
        }
    }
}
