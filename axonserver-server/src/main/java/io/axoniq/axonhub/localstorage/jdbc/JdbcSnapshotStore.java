package io.axoniq.axonhub.localstorage.jdbc;

import io.axoniq.axonhub.localstorage.EventTypeContext;

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
