package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@ConditionalOnProperty(name = "axoniq.axonserver.storage", havingValue = "jdbc")
@Component
public class JdbcEventStoreFactory implements EventStoreFactory {


    private final StorageProperties storageProperties;

    public JdbcEventStoreFactory(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
    }

    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        return new JdbcEventStore(new EventTypeContext(context, EventType.EVENT), storageProperties.dataSource());
    }

    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        return new JdbcSnapshotStore(new EventTypeContext(context, EventType.SNAPSHOT), storageProperties.dataSource());
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStorageEngine eventStorageEngine) {
        return new SingleInstanceTransactionManager(eventStorageEngine);
    }
}
