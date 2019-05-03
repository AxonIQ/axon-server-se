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
import io.axoniq.axonserver.localstorage.SerializedEvent;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * Maintains a cache of last sequence numbers per aggregate. Used to verify new events coming in, also considering pending
 * transactions.
 * @author Marc Gathier
 * @since 4.2
 */
public class SequenceNumberCache {

    private final BiFunction<String, Boolean, Optional<Long>> aggregateSequenceNumberProvider;
    private final Map<String, AtomicLong> sequenceNumbersPerAggregate;


    public SequenceNumberCache(BiFunction<String, Boolean, Optional<Long>> aggregateSequenceNumberProvider) {
        this(aggregateSequenceNumberProvider, 10_000);
    }

    public SequenceNumberCache(BiFunction<String, Boolean, Optional<Long>> aggregateSequenceNumberProvider,
                               int maxSize) {
        this.aggregateSequenceNumberProvider = aggregateSequenceNumberProvider;
        // TODO: map is not thread-safe
        sequenceNumbersPerAggregate = new LinkedHashMap<String, AtomicLong>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, AtomicLong> eldest) {
                return size() > maxSize;
            }
        };
    }

    /**
     * Reserve the sequence numbers of the aggregates in the provided list to avoid collisions during store.
     *
     * @param events list of events to store
     */
    public void reserveSequenceNumbers(List<SerializedEvent> events) {
        Map<String, MinMaxPair> minMaxPerAggregate = new HashMap<>();
        events.stream()
              .map(SerializedEvent::asEvent)
              .filter(this::isDomainEvent)
              .forEach(e -> minMaxPerAggregate.computeIfAbsent(e.getAggregateIdentifier(),
                                                               i -> new MinMaxPair(e.getAggregateIdentifier(),
                                                                                   e.getAggregateSequenceNumber()))
                                              .setMax(e.getAggregateSequenceNumber()));

        Map<String, Long> oldSequenceNumberPerAggregate = new HashMap<>();
        for (Map.Entry<String, MinMaxPair> entry : minMaxPerAggregate.entrySet()) {
            AtomicLong current = sequenceNumbersPerAggregate.computeIfAbsent(entry.getKey(),
                                                                             id -> new AtomicLong(
                                                                                     aggregateSequenceNumberProvider
                                                                                             .apply(id,
                                                                                                    entry.getValue()
                                                                                                         .getMin() > 0)
                                                                                             .orElse(-1L)));

            if (!current.compareAndSet(entry.getValue().getMin() - 1, entry.getValue().getMax())) {
                oldSequenceNumberPerAggregate.forEach((aggregateId, sequenceNumber) -> sequenceNumbersPerAggregate
                        .put(aggregateId, new AtomicLong(sequenceNumber)));
                throw new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE,
                                                     String.format(
                                                             "Invalid sequence number %d for aggregate %s, expected %d",
                                                             entry.getValue().getMin(),
                                                             entry.getKey(),
                                                             current.get() + 1));
            }
            oldSequenceNumberPerAggregate.putIfAbsent(entry.getKey(), entry.getValue().getMin() - 1);
        }
    }

    private boolean isDomainEvent(Event e) {
        return !StringUtils.isEmpty(e.getAggregateType());
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
}
