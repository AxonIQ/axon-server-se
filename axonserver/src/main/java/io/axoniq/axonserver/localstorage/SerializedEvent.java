package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Wrapper around an Event that keeps track of the Serialized form of the Event, to prevent unnecessary
 * (un)marshalling of Event messages.
 */
public class SerializedEvent {

    private final EventTransformer eventTransformer;
    private final byte[] serializedEvent;
    private volatile Event event;

    public SerializedEvent(Event event, EventTransformer eventTransformer) {
        this(eventTransformer.transform(event).getPayloadBytes(), eventTransformer);
        this.event = event;
    }

    public SerializedEvent(byte[] serializedEvent, EventTransformer eventTransformer) {
        this.serializedEvent = serializedEvent;
        this.eventTransformer = eventTransformer;
    }

    public InputStream asInputStream() {
        return new ByteArrayInputStream(serializedEvent);
    }

    public Event asEvent() {
        if (event == null) {
            event = eventTransformer.readEvent(serializedEvent);
        }
        return event;
    }

    public int size() {
        return serializedEvent.length;
    }

    public byte[] serializedData() {
        return serializedEvent;
    }

    public long getAggregateSequenceNumber() {
        return asEvent().getAggregateSequenceNumber();
    }

    public EventTransformer eventTransformer() {
        return eventTransformer;
    }
}
