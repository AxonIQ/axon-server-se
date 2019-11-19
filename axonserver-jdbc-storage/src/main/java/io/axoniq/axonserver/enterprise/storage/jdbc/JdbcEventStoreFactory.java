package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.enterprise.storage.jdbc.sync.StoreAlwaysSyncStrategy;
import io.axoniq.axonserver.enterprise.storage.jdbc.sync.StoreOnLeaderSyncStrategy;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;

import java.util.function.Predicate;

/**
 * Factory to create  {@link EventStorageEngine} instances that store data in a relational database.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class JdbcEventStoreFactory implements EventStoreFactory {


    private final StorageProperties storageProperties;
    private final MetaDataSerializer metaDataSerializer;
    private final MultiContextStrategy multiContextStrategy;
    private final Predicate<String> leaderProvider;

    public JdbcEventStoreFactory(StorageProperties storageProperties,
                                 MetaDataSerializer metaDataSerializer,
                                 MultiContextStrategy multiContextStrategy,
                                 Predicate<String> leaderProvider) {
        this.storageProperties = storageProperties;
        this.metaDataSerializer = metaDataSerializer;
        this.multiContextStrategy = multiContextStrategy;
        this.leaderProvider = leaderProvider;
    }

    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        return new JdbcEventStorageEngine(new EventTypeContext(context, EventType.EVENT),
                                          storageProperties.dataSource(),
                                          metaDataSerializer,
                                          multiContextStrategy,
                                          syncStrategy(context)) {
        };
    }

    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        return new JdbcEventStorageEngine(new EventTypeContext(context, EventType.SNAPSHOT), storageProperties.dataSource(),
                                     metaDataSerializer, multiContextStrategy,
                                          syncStrategy(context));
    }

    /**
     * Returns the sychronization strategy to use. Can be either store-on-leader-only (event store only stores events when it is leader) or
     * store-always (event store always stores the events, for instance when running in a cluster with multiple storage formats)
     * @return defined sychronization strategy
     */
    private SyncStrategy syncStrategy(String context) {
        if( storageProperties.isStoreOnLeaderOnly()) {
            return new StoreOnLeaderSyncStrategy(context, leaderProvider);
        }
        return new StoreAlwaysSyncStrategy();
    }


}
