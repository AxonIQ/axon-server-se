package io.axoniq.axonserver.migration;

/**
 * @author Marc Gathier
 */
public interface DomainEvent extends BaseEvent {
    long getGlobalIndex();
    String getType();

    String getAggregateIdentifier();

    long getSequenceNumber();


}
