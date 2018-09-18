package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.grpc.event.Event;
import org.springframework.util.StringUtils;

/**
 * Author: marc
 */
public class WrappedEvent implements ProcessedEvent {

    private final Event event;

    public WrappedEvent(Event event) {
        this.event = event;
    }

    @Override
    public int getSerializedSize() {
        return event.getSerializedSize();
    }

    @Override
    public byte[] toByteArray() {
        return event.toByteArray();
    }

    @Override
    public String getAggregateIdentifier() {
        return event.getAggregateIdentifier();
    }

    @Override
    public long getAggregateSequenceNumber() {
        return event.getAggregateSequenceNumber();
    }

    @Override
    public String getMessageIdentifier() {
        return event.getMessageIdentifier();
    }

    @Override
    public byte[] getPayloadBytes() {
        return event.getPayload().toByteArray();
    }

    @Override
    public String getPayloadRevision() {
        return event.getPayload().getRevision();
    }

    @Override
    public String getPayloadType() {
        return event.getPayload().getType();
    }

    @Override
    public long getTimestamp() {
        return event.getTimestamp();
    }

    @Override
    public String getAggregateType() {
        return event.getAggregateType();
    }

    @Override
    public boolean isDomainEvent() {
        return ! StringUtils.isEmpty(getAggregateIdentifier());
    }
}
