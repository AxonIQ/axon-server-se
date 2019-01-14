package io.axoniq.axonserver.connector;

/**
 * @author Marc Gathier
 */
public interface UnitOfWork {
    void publish(Event event);

    void commit();

    void rollback();
}
