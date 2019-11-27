package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Strategy to handle multi-context for RDBMS storage.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public interface MultiContextStrategy {

    /**
     * Returns a fully qualified table name for the event/snapshot table in a specific context.
     * @param eventTypeContext the type (event/snapshot) and context
     * @return the fully qualified table name
     */
    String getTableName(EventTypeContext eventTypeContext);

    /**
     * Initializes a context. Creates schemas and tables when necessary.
     * @param eventTypeContext the type (event/snapshot) and context
     * @param connection the database connection to use
     * @throws SQLException when not able to create/check the database objects
     */
    void init(EventTypeContext eventTypeContext, Connection connection) throws SQLException;

    /**
     * Checks if the specified context exists.
     *
     * @param context    the name of the context
     * @param connection the database connection to use
     * @return true if events table exists
     *
     * @throws SQLException
     */
    boolean exists(String context, Connection connection) throws SQLException;
}
