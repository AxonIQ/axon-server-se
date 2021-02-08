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
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.StringUtils;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Maintains a cache of last sequence numbers per aggregate. Used to verify new events coming in, also considering
 * pending transactions.
 * Cache is limited in the number of entries, eviction policy is least recently used.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class SequenceNumberCache {

    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1,
                                                                                                                new CustomizableThreadFactory(
                                                                                                                        "cache-cleanup"));
    private static final EventStorageEngine.SearchHint[] NO_HINTS = {};
    private static final EventStorageEngine.SearchHint[] SEARCH_RECENT = {EventStorageEngine.SearchHint.RECENT_ONLY};

    private final int maxSize;
    private final BiFunction<String, EventStorageEngine.SearchHint[], Optional<Long>> aggregateSequenceNumberProvider;
    private final Clock clock;
    private final Map<String, SequenceNumber> sequenceNumbersPerAggregate = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> cleanupTask;

    /**
     * Creates a sequence number cache with specified aggregateSequenceNumber provider and default clock and cache size.
     *
     * @param aggregateSequenceNumberProvider function to retrieve the last sequence number for an aggregate
     */
    public SequenceNumberCache(
            BiFunction<String, EventStorageEngine.SearchHint[], Optional<Long>> aggregateSequenceNumberProvider) {
        this(aggregateSequenceNumberProvider, Clock.systemUTC(), 100_000);
    }

    /**
     * Creates a sequence number cache with specified aggregateSequenceNumber provider, clock and cache size.
     *
     * @param aggregateSequenceNumberProvider function to retrieve the last sequence number for an aggregate
     * @param clock                           clock to use to set last used time
     * @param maxSize                         maximum number of entries for the cache
     */
    public SequenceNumberCache(
            BiFunction<String, EventStorageEngine.SearchHint[], Optional<Long>> aggregateSequenceNumberProvider,
            Clock clock, int maxSize) {
        this.aggregateSequenceNumberProvider = aggregateSequenceNumberProvider;
        this.clock = clock;
        this.maxSize = maxSize;
        this.cleanupTask = SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(() -> clearOld(TimeUnit.MINUTES.toMillis(30)),
                                                                          15,
                                                                          15,
                                                                          TimeUnit.MINUTES);
    }

    /**
     * Reserve the sequence numbers of the aggregates in the provided list to avoid collisions during store.
     *
     * @param events list of events to store
     */
    public void reserveSequenceNumbers(List<Event> events) {
        reserveSequenceNumbers(events, false);
    }

    /**
     * Reserve the sequence numbers of the aggregates in the provided list to avoid collisions during store.
     * <p>
     * Option {@code force} is used by enterprise edition to set sequence numbers for aggregates of transactions that
     * were
     * previously validated.
     *
     * @param events list of events to store
     * @param force  accept the sequence numbers from the events list as valid
     */
    public Runnable reserveSequenceNumbers(List<Event> events, boolean force) {
        Map<String, MinMaxPair> minMaxPerAggregate = new HashMap<>();
        events.stream()
              .filter(this::isDomainEvent)
              .forEach(e -> minMaxPerAggregate.computeIfAbsent(e.getAggregateIdentifier(),
                                                               i -> new MinMaxPair(e.getAggregateIdentifier(),
                                                                                   e.getAggregateSequenceNumber()))
                                              .setMax(e.getAggregateSequenceNumber()));

        Map<String, SequenceNumber> oldSequenceNumberPerAggregate = new HashMap<>();
        Runnable unreserve = () -> oldSequenceNumberPerAggregate
                .forEach(sequenceNumbersPerAggregate::put);
            for (Map.Entry<String, MinMaxPair> entry : minMaxPerAggregate.entrySet()) {
                if (force) {
                    sequenceNumbersPerAggregate.put(entry.getKey(), new SequenceNumber(entry.getValue().getMax()));
                } else {
                    SequenceNumber updated = sequenceNumbersPerAggregate.compute(entry.getKey(),
                                                                                 (aggId, old) -> checkAndUpdateSequenceNumber(
                                                                                         aggId,
                                                                                         old,
                                                                                         entry.getValue()));
                    if (!updated.isValid()) {
                        unreserve.run();
                        throw new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE,
                                                             String.format(
                                                                     "Invalid sequence number %d for aggregate %s, expected %d",
                                                                     entry.getValue().getMin(),
                                                                     entry.getKey(),
                                                                     updated.get() + 1));
                    }
                    oldSequenceNumberPerAggregate.putIfAbsent(entry.getKey(),
                                                              new SequenceNumber(entry.getValue().getMin() - 1));
                }
            }
        return unreserve;
    }

    private boolean isDomainEvent(Event e) {
        return !StringUtils.isEmpty(e.getAggregateType());
    }

    /**
     * Checks if the min sequence number for an aggregate has the correct value and updates the cache. If the min
     * sequence number is valid
     * it returns a new {@link SequenceNumber} object with max sequence number from minMaxPair.
     * If the min sequence number supplied in the minMaxPair parameter is not valid, it returns the current sequence
     * number, with a flag
     * indicating that the last check was invalid.
     *
     * @param aggregateIdentfier aggregate identifier to check
     * @param current            currently cached SequenceNumber for this aggregate
     * @param minMaxPair         min value to check and max value to set
     * @return updated SequenceNumber containing the new sequence number or updated valid flag.
     */
    private SequenceNumber checkAndUpdateSequenceNumber(String aggregateIdentfier, SequenceNumber current,
                                                        MinMaxPair minMaxPair) {
        if (current == null) {
            current = new SequenceNumber(aggregateSequenceNumberProvider
                                                 .apply(aggregateIdentfier, searchHints(minMaxPair.min)).orElse(-1L));
        }
        if (minMaxPair.getMin() == current.get() + 1) {
            return new SequenceNumber(minMaxPair.max);
        }

        return current.invalid();
    }

    /**
     * Determine the search hints to use in finding an aggregate. If the expected next sequence number is 0, we expect
     * the aggregate not to exist, in which case we may not want to scan the entire event store, but only the recent
     * events.
     *
     * @param expectedNextSequenceNumber expected next sequence number
     * @return search hints for finding the aggregate
     */
    private EventStorageEngine.SearchHint[] searchHints(long expectedNextSequenceNumber) {
        if (expectedNextSequenceNumber == 0) {
            return SEARCH_RECENT;
        }

        return NO_HINTS;
    }

    public void clear() {
        sequenceNumbersPerAggregate.clear();
    }

    /**
     * Removes entries from the cache that are older than timeout milliseconds. Does not clear anything if cache has not reached
     * its max size.
     * @param timeout timeout value
     */
    public void clearOld(long timeout) {
        if (sequenceNumbersPerAggregate.size() > maxSize) {
            long minTimestamp = clock.millis() - timeout;
            sequenceNumbersPerAggregate.entrySet().removeIf(e -> e.getValue().timestamp() < minTimestamp);
        }
    }

    /**
     * Stops the scheduled cleanupTask for the cache.
     */
    public void close() {
        if (cleanupTask != null && !cleanupTask.isDone()) {
            cleanupTask.cancel(true);
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
            return new SequenceNumber(sequence - i);
        }

        public SequenceNumber invalid() {
            if (!valid) {
                return this;
            }
            return new SequenceNumber(sequence, false);
        }
    }
}
