package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;

import java.util.List;

/**
 * @author Marc Gathier
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
