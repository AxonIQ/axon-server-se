package io.axoniq.axonserver.enterprise.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Marc Gathier
 */
public interface VendorSpecific {

    void createTableIfNotExists(String tableName, Connection connection) throws SQLException;

    void createTableIfNotExists(String schema, String table, Connection connection) throws SQLException;

    void createSchemaIfNotExists(String schema, Connection connection) throws SQLException;

    void dropSchema(String schema, Connection connection);
}
