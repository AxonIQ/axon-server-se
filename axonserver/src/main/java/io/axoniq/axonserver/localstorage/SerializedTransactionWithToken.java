package io.axoniq.axonserver.localstorage;

import java.util.List;
import java.util.Objects;

/**
 * Author: marc
 */
public class SerializedTransactionWithToken {

    private final long token;
    private final int version;
    private final List<SerializedEvent> events;
    private final long safePoint;
    private final long masterGeneration;

    public SerializedTransactionWithToken(long token,
                                          int version, List<SerializedEvent> events) {
        this(token, version, events, 0, 0);
    }

    public SerializedTransactionWithToken(long token, int version,
                                          List<SerializedEvent> events, long safePoint, long masterGeneration) {
        this.token = token;
        this.version = version;
        this.events = events;
        this.safePoint = safePoint;
        this.masterGeneration = masterGeneration;
    }

    public long getToken() {
        return token;
    }

    public List<SerializedEvent> getEvents() {
        return events;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SerializedTransactionWithToken that = (SerializedTransactionWithToken) o;
        return token == that.token &&
                Objects.equals(events, that.events);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, events);
    }

    public int getEventsCount() {
        return events.size();
    }

    public long getSafePoint() {
        return safePoint;
    }

    public long getMasterGeneration() {
        return masterGeneration;
    }
}
