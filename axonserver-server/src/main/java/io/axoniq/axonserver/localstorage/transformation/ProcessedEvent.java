package io.axoniq.axonserver.localstorage.transformation;

/**
 * Author: marc
 */
public interface ProcessedEvent {

    int getSerializedSize();

    byte[] toByteArray();

    String getAggregateIdentifier();

    long getAggregateSequenceNumber();

    String getMessageIdentifier();

    byte[] getPayloadBytes();

    String getPayloadRevision();

    String getPayloadType();

    long getTimestamp();

    String getAggregateType();

    boolean isDomainEvent();
}
