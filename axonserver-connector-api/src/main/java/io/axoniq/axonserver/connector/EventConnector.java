package io.axoniq.axonserver.connector;

import java.util.List;

/**
 * @author Marc Gathier
 */
public interface EventConnector {
    UnitOfWork createUnitOfWork();

    void publish(ConnectorEvent event);
}
