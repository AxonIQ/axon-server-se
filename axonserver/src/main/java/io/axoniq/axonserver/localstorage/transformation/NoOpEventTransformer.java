package io.axoniq.axonserver.localstorage.transformation;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;

/**
 * Author: marc
 */
public class NoOpEventTransformer extends EventTransformer {

    public static final NoOpEventTransformer INSTANCE = new NoOpEventTransformer();

    private NoOpEventTransformer() {

    }

    @Override
    protected Event transform(byte[] eventBytes) {
        try {
            return Event.parseFrom(eventBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Parsing of event failed", e);
        }
    }

    @Override
    public ProcessedEvent transform(Event event) {
        return new WrappedEvent(event);
    }
}
