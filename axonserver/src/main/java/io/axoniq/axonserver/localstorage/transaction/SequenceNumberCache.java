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
import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Maintains a cache of last sequence numbers per aggregate. Used to verify new events coming in, also considering pending
 * transactions.
 * @author Marc Gathier
 * @since 4.2
 */
public class SequenceNumberCache {

    private final int maxSize;
    private final BiFunction<String, Boolean, Optional<Long>> aggregateSequenceNumberProvider;
    private final Clock clock;
    private final Map<String, SequenceNumber> sequenceNumbersPerAggregate = new ConcurrentHashMap<>();


    public SequenceNumberCache(BiFunction<String, Boolean, Optional<Long>> aggregateSequenceNumberProvider) {
        this(aggregateSequenceNumberProvider, Clock.systemUTC(), 100_000);
    }

    public SequenceNumberCache(BiFunction<String, Boolean, Optional<Long>> aggregateSequenceNumberProvider, Clock clock, int maxSize) {
        this.aggregateSequenceNumberProvider = aggregateSequenceNumberProvider;
        this.clock = clock;
        this.maxSize = maxSize;
    }
    /**
     * Reserve the sequence numbers of the aggregates in the provided list to avoid collisions during store.
     *
     * @param events list of events to store
     */
    public void reserveSequenceNumbers(List<SerializedEvent> events) {
        Map<String, MinMaxPair> minMaxPerAggregate = new HashMap<>();
        events.stream()
              .filter(SerializedEvent::isDomainEvent)
              .map(SerializedEvent::asEvent)
              .forEach(e -> minMaxPerAggregate.computeIfAbsent(e.getAggregateIdentifier(),
                                                               i -> new MinMaxPair(e.getAggregateIdentifier(),
                                                                                   e.getAggregateSequenceNumber()))
                                              .setMax(e.getAggregateSequenceNumber()));

        Set<String> aggregatesUpdated = new HashSet<>();
        for (Map.Entry<String, MinMaxPair> entry : minMaxPerAggregate.entrySet()) {
            SequenceNumber updated = sequenceNumbersPerAggregate.compute(entry.getKey(),
                                                                         (aggId, old) -> updateSequenceNumber(aggId, old, entry.getValue()));
            if( ! updated.isValid() ) {
                aggregatesUpdated.forEach(sequenceNumbersPerAggregate::remove);
                throw new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE,
                                                     String.format(
                                                             "Invalid sequence number %d for aggregate %s, expected %d",
                                                             entry.getValue().getMin(),
                                                             entry.getKey(),
                                                             updated.get() + 1));
            }
            aggregatesUpdated.add(entry.getKey());
        }
    }

    private SequenceNumber updateSequenceNumber(String aggId, SequenceNumber old, MinMaxPair entry) {
        if( old == null) {
            old = new SequenceNumber(aggregateSequenceNumberProvider.apply(aggId, entry.min > 0).orElse(-1L));
        }
        if( entry.getMin() == old.get() + 1) {
            return new SequenceNumber(entry.max);
        }

        return old.invalid();
    }

    public void clear() {
        sequenceNumbersPerAggregate.clear();
    }

    public void clearOld(long timeout) {
        if( sequenceNumbersPerAggregate.size() > maxSize) {
            long minTimestamp = clock.millis() - timeout;
            sequenceNumbersPerAggregate.entrySet().removeIf(e -> e.getValue().timestamp() < minTimestamp);
        }
    }


    private class MinMaxPair {

        private final String key;
        private final long min;
        private volatile long max;

        MinMaxPair(String key, long min) {
            this.key = key;
            this.min = min;
            this.max = min - 1;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public void setMax(long max) {
            if (max != this.max + 1) {
                throw new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE,
                                                     String.format(
                                                             "Invalid sequence number %d for aggregate %s, expected %d",
                                                             max,
                                                             key,
                                                             this.max + 1));
            }
            this.max = max;
        }
    }

    private class SequenceNumber {

        private final long sequence;
        private final boolean valid;
        private final long timestamp;

        private SequenceNumber(long sequence) {
            this(sequence, true);
        }

        private SequenceNumber(long sequence, boolean valid) {
            this.sequence = sequence;
            this.valid = valid;
            this.timestamp = clock.millis();
        }

        public boolean isValid() {
            return valid;
        }

        public long get() {
            return sequence;
        }

        public long timestamp() {
            return timestamp;
        }

        public SequenceNumber minus(int i) {
            return new SequenceNumber(sequence -i);
        }

        public SequenceNumber invalid() {
            if( !valid) return this;
            return new SequenceNumber(sequence, false);
        }
    }
}
