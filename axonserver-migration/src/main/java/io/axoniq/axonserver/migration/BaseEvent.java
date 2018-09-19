package io.axoniq.axonserver.migration;

/**
 * Author: marc
 */
public interface BaseEvent {
    String getEventIdentifier();

    long getTimeStampAsLong();

    String getPayloadType();

    String getPayloadRevision();

    byte[] getPayload();

    byte[] getMetaData();
}
