package io.axoniq.axonhub.localstorage.jdbc;

import io.axoniq.axonhub.localstorage.EventTypeContext;

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
