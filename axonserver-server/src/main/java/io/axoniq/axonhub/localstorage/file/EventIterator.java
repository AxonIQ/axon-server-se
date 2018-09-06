package io.axoniq.axonhub.localstorage.file;

import io.axoniq.axonhub.localstorage.EventInformation;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Author: marc
 */
public abstract class EventIterator implements Iterator<EventInformation>, AutoCloseable {
    protected long currentSequenceNumber;
    protected final List<EventInformation> eventsInTransaction = new LinkedList<>();

    public void close() {

    }
    @Override
    public boolean hasNext() {
        boolean next = !eventsInTransaction.isEmpty() || readTransaction();
        if( !next) {
            close();
        }
        return next;
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

    public Long getTokenAfter(long instant) {
        if (hasNext()) {
            EventInformation event = next();
            if (event.getEvent().getTimestamp() <= instant) {
                long token = currentSequenceNumber + 1;
                while (hasNext() && next().getEvent().getTimestamp() <= instant) {
                    token++;
                }

                return token - 1;
            }
        }
        return null;
    }
}
