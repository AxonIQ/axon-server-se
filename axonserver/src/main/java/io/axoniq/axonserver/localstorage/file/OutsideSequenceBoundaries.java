package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.function.Predicate;

/**
 * @author Sara Pellegrini
 * @since
 */
public class OutsideSequenceBoundaries implements Predicate<SerializedEvent> {

    private final long minSequenceNumber;
    private final long maxSequenceNumber;

    public OutsideSequenceBoundaries(long minSequenceNumber, long maxSequenceNumber) {
        this.minSequenceNumber = minSequenceNumber;
        this.maxSequenceNumber = maxSequenceNumber;
    }

    @Override
    public boolean test(SerializedEvent event) {
        return event.getAggregateSequenceNumber() < minSequenceNumber
                || event.getAggregateSequenceNumber() >= maxSequenceNumber;
    }
}
