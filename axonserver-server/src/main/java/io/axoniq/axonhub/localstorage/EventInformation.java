package io.axoniq.axonhub.localstorage;

import io.axoniq.axondb.Event;
import io.axoniq.axondb.grpc.EventWithToken;

/**
 * Author: marc
 */
public class EventInformation {

    private final long token;
    private final int position;
    private final Event event;

    public EventInformation(long token, int position, Event event) {

        this.token = token;
        this.position = position;
        this.event = event;
    }

    public long getToken() {
        return token;
    }

    public int getPosition() {
        return position;
    }

    public Event getEvent() {
        return event;
    }

    public EventWithToken asEventWithToken() {
        return EventWithToken.newBuilder().setToken(token).setEvent(event).build();
    }
}
