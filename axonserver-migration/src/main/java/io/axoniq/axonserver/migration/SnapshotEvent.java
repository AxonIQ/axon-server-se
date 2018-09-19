package io.axoniq.axonserver.migration;

/**
 * Author: marc
 */
public interface SnapshotEvent extends BaseEvent {
    String getType();

    String getAggregateIdentifier();

    long getSequenceNumber();

    String getTimeStamp();

}
