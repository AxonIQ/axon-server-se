package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;

/**
 * @author Marc Gathier
 */
public class JdbcEventStoreFactory implements EventStoreFactory {


    private final StorageProperties storageProperties;
    private final StorageTransactionManagerFactory storageTransactionManagerFactory;
    private final MetaDataSerializer metaDataSerializer;
    private final MultiContextStrategy multiContextStrategy;
    private final SyncStrategy syncStrategy;

    public JdbcEventStoreFactory(StorageProperties storageProperties,
                                 StorageTransactionManagerFactory storageTransactionManagerFactory,
                                 MetaDataSerializer metaDataSerializer,
                                 MultiContextStrategy multiContextStrategy,
                                 SyncStrategy syncStrategy) {
        this.storageProperties = storageProperties;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
        this.metaDataSerializer = metaDataSerializer;
        this.multiContextStrategy = multiContextStrategy;
        this.syncStrategy = syncStrategy;
    }

    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        return new JdbcEventStore(new EventTypeContext(context, EventType.EVENT), storageProperties.dataSource(),
                                  metaDataSerializer, multiContextStrategy,
                                  syncStrategy);
    }

    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        return new JdbcSnapshotStore(new EventTypeContext(context, EventType.SNAPSHOT), storageProperties.dataSource(),
                                     metaDataSerializer, multiContextStrategy,
                                     syncStrategy);
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStorageEngine eventStorageEngine) {
        return storageTransactionManagerFactory.createTransactionManager(eventStorageEngine);
    }
}
