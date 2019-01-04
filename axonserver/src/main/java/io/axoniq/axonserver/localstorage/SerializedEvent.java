package io.axoniq.axonserver.localstorage;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.commons.compress.utils.IOUtils.toByteArray;

/**
 * Wrapper around an Event that keeps track of the Serialized form of the Event, to prevent unnecessary
 * (un)marshalling of Event messages.
 */
public class SerializedEvent {

    private final byte[] serializedEvent;
    private volatile Event event;

    public SerializedEvent(Event event) {
        this.serializedEvent = event.toByteArray();
        this.event = event;
    }

    public SerializedEvent(byte[] eventFromFile) {
        this.serializedEvent = eventFromFile;
    }

    public SerializedEvent(InputStream event) {
        try {
            this.serializedEvent = toByteArray(event);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    public InputStream asInputStream() {
        return new ByteArrayInputStream(serializedEvent);
    }

    public Event asEvent() {
        if (event == null) {
            try {
                event = Event.parseFrom(serializedEvent);
            } catch (InvalidProtocolBufferException e) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
            }
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
}
