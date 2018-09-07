package io.axoniq.axonserver.localstorage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

import javax.sql.DataSource;

/**
 * Author: marc
 */
public class JdbcSnapshotStore extends JdbcAbstractStore{

    public JdbcSnapshotStore(EventTypeContext eventTypeContext,
                             DataSource dataSource) {
        super(eventTypeContext, dataSource);
    }

    protected String getTableName() {
        return "SNAPSHOT_EVENT_ENTRY";
    }

}
