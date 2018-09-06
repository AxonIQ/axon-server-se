package io.axoniq.axonhub.connector;

/**
 * Author: marc
 */
public interface UnitOfWork {
    void publish(Event event);

    void commit();

    void rollback();
}
