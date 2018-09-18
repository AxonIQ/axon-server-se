package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.connector.Event;
import io.axoniq.axonserver.grpc.MetaDataValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: marc
 */
public class GrpcBackedEvent implements Event {
    private final io.axoniq.axonserver.grpc.event.Event wrapped;

    public GrpcBackedEvent(io.axoniq.axonserver.grpc.event.Event wrapped) {
        this.wrapped = wrapped;
    }

    private Object unwrap(MetaDataValue value) {
        switch(value.getDataCase()) {
            case TEXT_VALUE:
                return value.getTextValue();
            case NUMBER_VALUE:
                return value.getNumberValue();
            case BOOLEAN_VALUE:
                return value.getBooleanValue();
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            case BYTES_VALUE:
                return "bytes";
            default:
                return null;
        }
    }

    @Override
    public Map<String, Object> getMetaData() {
        Map<String, Object> headers = new HashMap<>();
        wrapped.getMetaDataMap().forEach((key, value)-> headers.put(key, unwrap(value)));
        return headers;
    }

    @Override
    public byte[] getPayload() {
        return wrapped.getPayload().getData().toByteArray();
    }

    @Override
    public String getIdentifier() {
        return wrapped.getMessageIdentifier();
    }

    @Override
    public String getAggregateType() {
        return wrapped.getAggregateType();
    }

    @Override
    public String getPayloadType() {
        return wrapped.getPayload().getType();
    }

    @Override
    public String getPayloadRevision() {
        return wrapped.getPayload().getRevision();
    }

    @Override
    public long getTimestamp() {
        return wrapped.getTimestamp();
    }

    @Override
    public boolean isDomainEvent() {
        return ! wrapped.getAggregateIdentifier().isEmpty();
    }

    @Override
    public String getAggregateIdentifier() {
        return wrapped.getAggregateIdentifier();
    }

    @Override
    public long getSequenceNumber() {
        return wrapped.getAggregateSequenceNumber();
    }

    @Override
    public String getType() {
        return wrapped.getAggregateType();
    }
}
