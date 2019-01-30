package io.axoniq.axonserver.migration;

/**
 * @author Marc Gathier
 */
public interface SnapshotEvent extends BaseEvent {
    String getType();

    String getAggregateIdentifier();

    long getSequenceNumber();

    String getTimeStamp();

}
