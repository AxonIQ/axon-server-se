/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.MessagingPlatformException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marc Gathier
 */
public abstract class EventIterator implements Iterator<EventInformation>, AutoCloseable {
    protected long currentSequenceNumber;
    protected final List<EventInformation> eventsInTransaction = new LinkedList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public void close() {
        closed.set(true);
        doClose();
    }

    protected abstract void doClose();

    @Override
    public boolean hasNext() {
        if (closed.get()) throw new IllegalStateException("Iterator is closed");
        return !eventsInTransaction.isEmpty() || tryReadTransaction();
    }

    private boolean tryReadTransaction() {
        try {
            return readTransaction();
        } catch (MessagingPlatformException readException) {
            if( closed.get()) {
                throw new IllegalStateException("Iterator is closed");
            }
            throw readException;
        }
    }

    protected abstract boolean readTransaction();

    @Override
    public EventInformation next() {
        if (eventsInTransaction.isEmpty()) throw new NoSuchElementException();
        return eventsInTransaction.remove(0);
    }

    public List<EventInformation> pendingEvents() {
        return eventsInTransaction;
    }

    public Long getTokenAt(long instant) {
        if (hasNext()) {
            EventInformation event = next();
            if (event.getEvent().getTimestamp() == instant) {
                return event.getToken();
            }
            if (event.getEvent().getTimestamp() < instant) {
                while (hasNext()) {
                    event = next();
                    if (event.getEvent().getTimestamp() >= instant) {
                        return event.getToken();
                    }
                }
                return event.getToken() + 1;
            }
        }
        return null;
    }

}
