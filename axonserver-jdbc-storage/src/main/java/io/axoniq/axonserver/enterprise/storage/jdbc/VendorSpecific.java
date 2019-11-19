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

    default String fullyQualified(String tableName) {
        return tableName;
    }

    /**
     * Checks if the specified table exists in the specified schema.
     *
     * @param schema     the schema to check
     * @param table      the table to check
     * @param connection the connection to use
     * @return true if the table exists
     *
     * @throws SQLException
     */
    boolean tableExists(String schema, String table, Connection connection) throws SQLException;

    /**
     * Checks if the specified table exists in the user's default schema.
     *
     * @param tableName  the table to check
     * @param connection the connection to use
     * @return true if the table exists
     *
     * @throws SQLException
     */
    boolean tableExists(String tableName, Connection connection) throws SQLException;
}
