/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.List;

/**
 * @author Marc Gathier
 */
public interface EventSource extends AutoCloseable {

    SerializedEvent readEvent(int position);

    default void close() {
        // no-action
    }

    TransactionIterator createTransactionIterator(long segment, long token, boolean validating);

    EventIterator createEventIterator(long segment, long startToken);

    default SerializedEvent readEvent(List<Integer> positions, long minSequenceNumber, long maxSequenceNumber) {
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
