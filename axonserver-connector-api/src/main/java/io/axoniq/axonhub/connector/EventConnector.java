package io.axoniq.axonhub.connector;

/**
 * Author: marc
 */
public interface EventConnector {
    UnitOfWork createUnitOfWork();

    void publish(Event event);
}
