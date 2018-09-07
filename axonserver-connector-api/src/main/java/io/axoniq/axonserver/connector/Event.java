package io.axoniq.axonserver.connector;

import java.util.Map;

/**
 * Author: marc
 */
public interface Event {
    Map<String, Object> getMetaData();

    byte[] getPayload();

    String getIdentifier();

    String getAggregateType();

    String getPayloadType();

    String getPayloadRevision();

    long getTimestamp();

    boolean isDomainEvent();

    String getAggregateIdentifier();

    long getSequenceNumber();

    String getType();
}
