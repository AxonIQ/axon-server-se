package io.axoniq.axonserver.connector;

/**
 * Author: marc
 */
public interface UnitOfWork {
    void publish(Event event);

    void commit();

    void rollback();
}
