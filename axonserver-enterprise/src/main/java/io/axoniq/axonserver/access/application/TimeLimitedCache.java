package io.axoniq.axonserver.access.application;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: marc
 */
public class TimeLimitedCache<T1, T2> {
    final Map<T1, ValueWrapper<T2>> entries = new ConcurrentHashMap<>();


    private final long timeToLive;

    public TimeLimitedCache(long timeToLive) {
        this.timeToLive = timeToLive;
    }

    static class ValueWrapper<T2> {
        final T2 value;
        final long evictTimestamp;

        public ValueWrapper(T2 value, long evictTimestamp) {
            this.value = value;
            this.evictTimestamp = evictTimestamp;
        }
    }

    public void put(T1 key, T2 value) {
        entries.put(key, new ValueWrapper<>(value, System.currentTimeMillis() + timeToLive));
    }

    public T2 get(T1 key) {
        ValueWrapper<T2> value = entries.get(key);
        if (value == null) return null;
        if (value.evictTimestamp < System.currentTimeMillis()) {
            entries.remove(key);
            return null;
        }
        return value.value;
    }

}
