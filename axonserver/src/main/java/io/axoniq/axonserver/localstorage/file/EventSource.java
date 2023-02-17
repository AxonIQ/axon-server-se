/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.EventStoreValidationException;
import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.List;

/**
 * Interface for an event store segment.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public interface EventSource extends AutoCloseable {

    /**
     * Reads an event from this segment.
     *
     * @param position the offset in the segment
     * @return the event
     */
    SerializedEvent readEvent(int position);

    /**
     * Closes this segment.
     */
    default void close() {
        // no-action
    }

    /**
     * Creates an iterator to iterate through the transactions stored in this segment.
     *
     * @param startToken the token of the first transaction to include in this iterator
     * @param validating while reading validate the transaction checksums
     * @return iterator for event store transactions
     * @throws EventStoreValidationException when the startToken is not the first token in a transaction
     */
    TransactionIterator createTransactionIterator(long startToken, boolean validating);

    /**
     * Creates an iterator to iterate through the events stored in this segment.
     *
     * @param startToken the token of the first event to include in this iterator
     * @return iterator for events
     */
    EventIterator createEventIterator(long startToken);

    /**
     * Creates an iterator to iterate through the events stored in this segment. Starts with the first event in this
     * segment.
     *
     * @return iterator for events
     */
    default EventIterator createEventIterator() {
        return createEventIterator(segment());
    }

    /**
     * Returns the segment number of this {@link EventSource}.
     *
     * @return the segment number
     */
    long segment();

    /**
     * Reads the last event from this event store segment that matches the constraint for the min/max sequence number.
     * The {@code positions} list contains a list of offsets of the events in the file.
     *
     * @param minSequenceNumber the minimum sequence number of the event (inclusive)
     * @param maxSequenceNumber the maximum sequence number of the event (exclusive)
     * @param positions         the positions of the events in the segment
     * @return the last matching event or null if not found
     */
    default SerializedEvent readLastInRange(long minSequenceNumber, long maxSequenceNumber,
                                            List<Integer> positions) {
        for (int i = positions.size() - 1; i >= 0; i--) {
            SerializedEvent event = readEvent(positions.get(i));
            if (event.getAggregateSequenceNumber() >= minSequenceNumber
                    && event.getAggregateSequenceNumber() < maxSequenceNumber) {
                return event;
            }
        }

        return null;
    }
}
