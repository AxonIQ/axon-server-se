package io.axoniq.axonserver.localstorage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

import javax.sql.DataSource;

/**
 * Author: marc
 */
public class JdbcEventStore extends JdbcAbstractStore{

    public JdbcEventStore(EventTypeContext eventTypeContext,
                          DataSource dataSource) {
        super(eventTypeContext, dataSource);
    }

    protected String getTableName() {
        return "DOMAIN_EVENT_ENTRY";
    }

}
