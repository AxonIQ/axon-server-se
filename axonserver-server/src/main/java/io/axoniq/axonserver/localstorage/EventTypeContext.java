package io.axoniq.axonserver.localstorage;

import java.util.Objects;

/**
 * Author: marc
 */
public class EventTypeContext {

    private final String context;
    private final EventType eventType;

    public EventTypeContext(String context, EventType eventType) {
        this.context = context;
        this.eventType = eventType;
    }


    public String getContext() {
        return context;
    }

    public EventType getEventType() {
        return eventType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventTypeContext that = (EventTypeContext) o;
        return Objects.equals(context, that.context) &&
                eventType == that.eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, eventType);
    }

    @Override
    public String toString() {
        return context + '-' + eventType;
    }
}
