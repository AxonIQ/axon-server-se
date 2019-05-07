package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Marc Gathier
 */
public interface MultiContextStrategy {

    String getTableName(EventTypeContext eventTypeContext);

    void init(EventTypeContext eventTypeContext, Connection connection) throws SQLException;
}
