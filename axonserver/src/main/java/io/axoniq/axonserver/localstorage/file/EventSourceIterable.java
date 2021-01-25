package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since
 */
class EventSourceIterable implements Iterable<SerializedEvent> {

    private final EventSource eventSource;
    private final List<Integer> positions;
    private final Predicate<SerializedEvent> continueUtntil;
    private final Iterable<SerializedEvent> iterable;


    public EventSourceIterable(List<Integer> positions,
                               EventSource eventSource,
                               long minSequence,
                               long maxSequence,
                               long maxResults) {
        this(positions,
             eventSource,
             new SequenceBoundaries(minSequence, maxSequence),
             maxResults);
    }


    public EventSourceIterable(List<Integer> positions,
                               EventSource eventSource,
                               Predicate<SerializedEvent> valid,
                               long maxResults) {
        this.positions = positions;
        this.eventSource = eventSource;
        this.continueUtntil = valid;
        this.iterable = new LimitedItemsIterable<>(maxResults, EventIterator::new);
    }

    @Nonnull
    @Override
    public Iterator<SerializedEvent> iterator() {
        return iterable.iterator();
    }

    private class EventIterator implements Iterator<SerializedEvent> {

        private int nextPosition = 0;
        private SerializedEvent next = prefetch();

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public SerializedEvent next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            SerializedEvent value = next;
            next = prefetch();
            return value;
        }

        private SerializedEvent prefetch() {
            if (nextPosition < positions.size()) {
                SerializedEvent event = eventSource.readEvent(positions.get(nextPosition++));
                if (continueUtntil.test(event)) {
                    return event;
                }
            }
            return null;
        }
    }
}
