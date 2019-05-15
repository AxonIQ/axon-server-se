package io.axoniq.axonserver.enterprise.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Vendor specific handling of creation of tables and schemas.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public interface VendorSpecific {

    /**
     * Creates an event table with tableName in the current schema if it does not yet exist.
     * @param tableName the table name
     * @param connection the connection to use
     * @throws SQLException when not able to check or create the table
     */
    void createTableIfNotExists(String tableName, Connection connection) throws SQLException;

    /**
     * Creates an event table with name table in the specified schema if the table does not yet exist.
     * @param schema the schema name
     * @param table the table name
     * @param connection the connection to use
     * @throws SQLException when not able to check or create the table
     */
    void createTableIfNotExists(String schema, String table, Connection connection) throws SQLException;

    /**
     * Creates a schema if the schema does not yet exist.
     * @param schema the name of the schema
     * @param connection the connection to use
     * @throws SQLException when not able to check or create the schema
     */
    void createSchemaIfNotExists(String schema, Connection connection) throws SQLException;

    /**
     * Drops a schema.
     * @param schema the name of the schema
     * @param connection the connection to use
     */
    void dropSchema(String schema, Connection connection);
}
