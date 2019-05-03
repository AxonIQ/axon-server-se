package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

import javax.sql.DataSource;

/**
 * @author Marc Gathier
 */
public class JdbcSnapshotStore extends JdbcAbstractStore{

    public JdbcSnapshotStore(EventTypeContext eventTypeContext,
                             DataSource dataSource,
                             MetaDataSerializer metaDataSerializer,
                             MultiContextStrategy multiContextStrategy,
                             SyncStrategy syncStrategy) {
        super(eventTypeContext, dataSource, metaDataSerializer, multiContextStrategy, syncStrategy);
    }

}
