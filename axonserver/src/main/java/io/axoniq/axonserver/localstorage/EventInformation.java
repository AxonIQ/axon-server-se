package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;

/**
 * @author Marc Gathier
 */
public class EventInformation {

    private final int position;
    private final SerializedEventWithToken event;

    public EventInformation(int position, SerializedEventWithToken event) {
        this.position = position;
        this.event = event;
    }

    public long getToken() {
        return event.getToken();
    }

    public int getPosition() {
        return position;
    }

    public Event getEvent() {
        return event.asEvent();
    }

    public EventWithToken asEventWithToken() {
        return event.asEventWithToken();
    }

    public SerializedEventWithToken getSerializedEventWithToken() {
        return event;
    }
}
