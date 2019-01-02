package io.axoniq.axonserver.localstorage;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;

import java.io.*;

/**
 * Wrapper around an EventWithToken that keeps track of the Serialized form of the Event, to prevent unnecessary
 * (un)marshalling of Event messages.
 */
public class SerializedEventWithToken {

    private final long token;
    private final SerializedEvent serializedEvent;

    public SerializedEventWithToken(long token, SerializedEvent event) {
        this.serializedEvent = event;
        this.token = token;
    }

    public SerializedEventWithToken(long token, Event event, EventTransformer eventTransformer) {
        this(token, new SerializedEvent(event, eventTransformer));
    }

    public InputStream asInputStream() {
        byte[] bytes = new byte[serializedEvent.size() + 32]; // extra space for tags, token and some to spare(see sizes below)
        CodedOutputStream cos = CodedOutputStream.newInstance(bytes);
        try {
            if (token != 0L) {
                cos.writeInt64(EventWithToken.TOKEN_FIELD_NUMBER, token); // max 14 bytes long
            }
            // 'manually' encode the event, as we have the serialized form already
            cos.writeTag(EventWithToken.EVENT_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED); // max 5 bytes
            cos.writeUInt32NoTag(serializedEvent.size()); // max 5 bytes
            cos.writeRawBytes(serializedEvent.serializedData());
            cos.flush();
            return new ByteArrayInputStream(bytes, 0, cos.getTotalBytesWritten());
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Unable to writed to Coded Stream", e);
        }
    }

    public long getToken() {
        return token;
    }

    public EventWithToken asEventWithToken() {
        return EventWithToken.newBuilder().setToken(token).setEvent(asEvent()).build();
    }

    public Event asEvent() {
        return serializedEvent.asEvent();
    }
}
