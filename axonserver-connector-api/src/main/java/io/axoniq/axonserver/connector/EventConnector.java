package io.axoniq.axonserver.connector;

import java.util.List;

/**
 * Author: marc
 */
public interface EventConnector {
    UnitOfWork createUnitOfWork();

    void publish(ConnectorEvent event);
}
