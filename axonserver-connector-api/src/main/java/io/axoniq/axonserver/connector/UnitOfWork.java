package io.axoniq.axonserver.connector;

import java.util.List;

/**
 * @author Marc Gathier
 */
public interface UnitOfWork {
    void publish(List<? extends ConnectorEvent> event);

    void commit();

    void rollback();
}
