package io.axoniq.axonserver.connector;

import java.util.Map;

/**
 * @author Marc Gathier
 */
public interface ConnectorEvent {
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

    Map<String, Object> getMetaData();
}
