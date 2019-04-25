/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains a cache of last sequence numbers per aggregate. Used to verify new events coming in, also considering pending
 * transactions.
 * @author Marc Gathier
 * @since 4.2
 */
public class SequenceNumberCache {
    private final EventStorageEngine eventStorageEngine;
    private final Map<String, AtomicLong> sequenceNumbersPerAggregate = new ConcurrentHashMap<>();


    public SequenceNumberCache(EventStorageEngine eventStorageEngine) {
        this.eventStorageEngine = eventStorageEngine;
    }

    /**
     * Reserve the sequence numbers of the aggregates in the provided list to avoid collisions during store.
     * @param events list of events to store
     */
    public void reserveSequenceNumbers(List<SerializedEvent> events) {
        Map<String, MinMaxPair> minMaxPerAggregate = new HashMap<>();
        events.stream()
              .map(SerializedEvent::asEvent)
              .filter(e -> isDomainEvent(e))
              .forEach(e -> minMaxPerAggregate.computeIfAbsent(e.getAggregateIdentifier(), i -> new MinMaxPair(e.getAggregateIdentifier(), e.getAggregateSequenceNumber())).setMax(e.getAggregateSequenceNumber()));

        Map<String, Long> oldSequenceNumberPerAggregate = new HashMap<>();
        for( Map.Entry<String, MinMaxPair> entry : minMaxPerAggregate.entrySet()) {
            AtomicLong current = sequenceNumbersPerAggregate.computeIfAbsent(entry.getKey(),
                                                                             id -> new AtomicLong(eventStorageEngine.getLastSequenceNumber(id, entry.getValue().getMin() > 0).orElse(-1L)));

            if( ! current.compareAndSet(entry.getValue().getMin() - 1, entry.getValue().getMax())) {
                oldSequenceNumberPerAggregate.forEach((aggregateId, sequenceNumber) -> sequenceNumbersPerAggregate.put(aggregateId, new AtomicLong(sequenceNumber)));
                throw new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE, String.format("Invalid sequence number %d for aggregate %s, expected %d",
                                                                                               entry.getValue().getMin(), entry.getKey(), current.get()+1));
            }
            oldSequenceNumberPerAggregate.putIfAbsent(entry.getKey(), entry.getValue().getMin() - 1);
        }
    }

    private boolean isDomainEvent(Event e) {
        return ! StringUtils.isEmpty(e.getAggregateIdentifier());
    }

    public void clear() {
        sequenceNumbersPerAggregate.clear();
    }


    private class MinMaxPair {

        private final String key;
        private final long min;
        private volatile long max;

        MinMaxPair(String key, long min) {
            this.key = key;
            this.min = min;
            this.max = min-1;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public void setMax(long max) {
            if( max != this.max + 1) {
                throw new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE, String.format("Invalid sequence number %d for aggregate %s, expected %d",
                                                                                               max, key, this.max+1));

            }
            this.max = max;
        }
    }
}
