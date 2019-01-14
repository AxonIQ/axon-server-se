package io.axoniq.axonserver.connector;

/**
 * @author Marc Gathier
 */
public interface EventConnector {
    UnitOfWork createUnitOfWork();

    void publish(Event event);
}
