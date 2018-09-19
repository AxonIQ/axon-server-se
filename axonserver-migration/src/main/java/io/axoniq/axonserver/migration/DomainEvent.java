package io.axoniq.axonserver.migration;

/**
 * Author: marc
 */
public interface DomainEvent extends BaseEvent {
    long getGlobalIndex();
    String getType();

    String getAggregateIdentifier();

    long getSequenceNumber();


}
