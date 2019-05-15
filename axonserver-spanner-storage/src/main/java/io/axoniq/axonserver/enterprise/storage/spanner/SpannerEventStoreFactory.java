package io.axoniq.axonserver.enterprise.storage.spanner;

import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.storage.spanner.serializer.ProtoMetaDataSerializer;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;

/**
 * Factory to create  {@link EventStorageEngine} instances that store data in a relational database.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class SpannerEventStoreFactory implements EventStoreFactory {


    private final StorageProperties storageProperties;
    private final StorageTransactionManagerFactory storageTransactionManagerFactory;
    private final RaftLeaderProvider leaderProvider;
    private final MetaDataSerializer metaDataSerializer;

    public SpannerEventStoreFactory(StorageProperties storageProperties,
                                    StorageTransactionManagerFactory storageTransactionManagerFactory,
                                    RaftLeaderProvider leaderProvider) {
        this.storageProperties = storageProperties;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
        this.leaderProvider = leaderProvider;
        this.metaDataSerializer = new ProtoMetaDataSerializer();
    }

    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        return new SpannerEventStorageEngine(new EventTypeContext(context, EventType.EVENT),
                                             storageProperties,
                                             metaDataSerializer, leaderProvider::isLeader) {
        };
    }

    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        return new SpannerEventStorageEngine(new EventTypeContext(context, EventType.SNAPSHOT),
                                             storageProperties,
                                             metaDataSerializer, leaderProvider::isLeader);
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStorageEngine eventStorageEngine) {
        return storageTransactionManagerFactory.createTransactionManager(eventStorageEngine);
    }
}
