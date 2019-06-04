package io.axoniq.axonserver.enterprise.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utility operations for JDBC connections.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class JdbcUtils {

    /**
     * Execute all statements provided.
     * @param connection the database connections
     * @param statements the statements
     * @throws SQLException when execution of one of the statements fails
     */
    public static void executeStatements(Connection connection, String... statements) throws SQLException {
        if( statements.length == 0) return;

        for (String sql: statements) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

}
