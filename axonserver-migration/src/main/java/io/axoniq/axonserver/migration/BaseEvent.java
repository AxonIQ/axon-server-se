package io.axoniq.axonserver.migration;

/**
 * @author Marc Gathier
 */
public interface BaseEvent {
    String getEventIdentifier();

    long getTimeStampAsLong();

    String getPayloadType();

    String getPayloadRevision();

    byte[] getPayload();

    byte[] getMetaData();
}
