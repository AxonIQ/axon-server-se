/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import java.time.Clock;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Cache for items that have an expiration date.
 *
 * @author Marc Gathier
 * @since 4.4.6
 */
public class TimeLimitedCache<T1, T2> {

    private final Map<T1, ValueWrapper<T2>> entries = new ConcurrentHashMap<>();
    private final Clock clock;
    private final long timeToLive;

    /**
     * Constructs a cache with a specified time to live for each item added.
     *
     * @param timeToLive the time in milliseconds that the entries are valid
     */
    public TimeLimitedCache(Clock clock, long timeToLive) {
        this.clock = clock;
        this.timeToLive = timeToLive;
    }

    /**
     * Returns collection of non-expired values in the cache.
     *
     * @return collection of non-expired values in the cache
     */
    public Collection<T2> values() {
        removeExpired();
        return entries.values().stream().map(v -> v.value).collect(Collectors.toList());
    }

    /**
     * Checks if the cache contains non-expired entries.
     *
     * @return true of there are no non-expired entries in the cache
     */
    public boolean isEmpty() {
        removeExpired();
        return entries.isEmpty();
    }

    /**
     * Puts an entry in the cache and calculates its expiry time.
     *
     * @param key   the key of the entry
     * @param value the value for the entry
     */
    public void put(T1 key, T2 value) {
        entries.put(key, new ValueWrapper<>(value, clock.millis() + timeToLive));
    }

    /**
     * Gets an item from the cache. Does not return the entry if it has expired.
     *
     * @param key the key of the entry
     * @return the value for the entry or null if not found
     */
    public T2 get(T1 key) {
        ValueWrapper<T2> value = entries.get(key);
        if (value == null) {
            return null;
        }
        if (timedOut(value)) {
            entries.remove(key);
            return null;
        }
        return value.value;
    }

    /**
     * Removes entries from the cache for which the predicate is true.
     *
     * @param predicate predicate to check if item must be removed
     */
    public void removeIf(BiPredicate<T1, T2> predicate) {
        entries.entrySet().removeIf(e -> predicate.test(e.getKey(), e.getValue().value));
    }

    private void removeExpired() {
        entries.entrySet().removeIf(e -> timedOut(e.getValue()));
    }

    private boolean timedOut(ValueWrapper<T2> value) {
        return value.evictTimestamp < clock.millis();
    }


    private static class ValueWrapper<T2> {

        final T2 value;
        final long evictTimestamp;

        public ValueWrapper(T2 value, long evictTimestamp) {
            this.value = value;
            this.evictTimestamp = evictTimestamp;
        }
    }
}
