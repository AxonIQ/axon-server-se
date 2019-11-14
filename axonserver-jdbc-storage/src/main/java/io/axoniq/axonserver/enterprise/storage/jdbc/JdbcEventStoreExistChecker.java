package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventStoreExistChecker;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Utility to check if a certain context already exists in the EventStore, without actually initializing it.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class JdbcEventStoreExistChecker implements EventStoreExistChecker {

    private final MultiContextStrategy multiContextStrategy;
    private final DataSource dataSource;

    public JdbcEventStoreExistChecker(MultiContextStrategy multiContextStrategy, DataSource dataSource) {
        this.multiContextStrategy = multiContextStrategy;
        this.dataSource = dataSource;
    }

    @Override
    public boolean exists(String context) {
        try (Connection connection = dataSource.getConnection()) {
            return multiContextStrategy.exists(context, connection);
        } catch (SQLException e) {
            return false;
        }
    }
}
