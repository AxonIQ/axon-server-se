package io.axoniq.axonhub.localstorage.transaction;

import io.axoniq.axonhub.localstorage.transformation.ProcessedEvent;

import java.util.List;

/**
 * Author: marc
 */
public class PreparedTransaction {
    private final long token;
    private final List<ProcessedEvent> eventList;

    public PreparedTransaction(long token, List<ProcessedEvent> eventList) {
        this.token = token;
        this.eventList = eventList;
    }

    public long getToken() {
        return token;
    }

    public List<ProcessedEvent> getEventList() {
        return eventList;
    }
}
