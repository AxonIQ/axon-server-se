package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 4.5
 */
class EventSourceIterable implements Iterable<SerializedEvent> {

    private final EventSource eventSource;
    private final List<Integer> positions;
    private final Predicate<SerializedEvent> completeCondition;
    private final Iterable<SerializedEvent> iterable;

    private final Logger logger = LoggerFactory.getLogger(EventSourceIterable.class);

    public EventSourceIterable(List<Integer> positions,
                               EventSource eventSource,
                               long minSequence,
                               long maxSequence) {
        this(positions,
             eventSource,
             new OutsideSequenceBoundaries(minSequence, maxSequence));
    }


    public EventSourceIterable(List<Integer> positions,
                               EventSource eventSource,
                               Predicate<SerializedEvent> completeCondition) {
        this.positions = positions;
        this.eventSource = eventSource;
        this.completeCondition = completeCondition;
        this.iterable = EventIterator::new;
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
                logger.trace("Reading event from EventSource in the thread {}", Thread.currentThread().getName());
                SerializedEvent event = eventSource.readEvent(positions.get(nextPosition++));
                if (!completeCondition.test(event)) {
                    return event;
                }
            }
            return null;
        }
    }
}
