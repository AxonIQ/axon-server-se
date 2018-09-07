package io.axoniq.axonserver.connector;

/**
 * Author: marc
 */
public interface EventConnector {
    UnitOfWork createUnitOfWork();

    void publish(Event event);
}
