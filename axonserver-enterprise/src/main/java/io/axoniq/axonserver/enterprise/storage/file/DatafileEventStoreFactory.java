package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.PrimaryEventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

import java.io.File;

/**
 * @author Marc Gathier
 */
public class DatafileEventStoreFactory implements EventStoreFactory {
    protected final EmbeddedDBProperties embeddedDBProperties;
    protected final EventTransformerFactory eventTransformerFactory;
    protected final StorageTransactionManagerFactory storageTransactionManagerFactory;

    public DatafileEventStoreFactory(EmbeddedDBProperties embeddedDBProperties, EventTransformerFactory eventTransformerFactory,
                                     StorageTransactionManagerFactory storageTransactionManagerFactory) {
        this.embeddedDBProperties = embeddedDBProperties;
        this.eventTransformerFactory = eventTransformerFactory;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
    }

    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getEvent());
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT), indexManager, eventTransformerFactory, embeddedDBProperties.getEvent());
        SecondaryEventStore second = new SecondaryEventStore(new EventTypeContext(context, EventType.EVENT), indexManager,
                                                             eventTransformerFactory,
                                                             embeddedDBProperties.getEvent());
        first.next(second);
        return first;
    }

    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getSnapshot());
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT), indexManager, eventTransformerFactory, embeddedDBProperties.getSnapshot());
        SecondaryEventStore second = new SecondaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT), indexManager,
                                                             eventTransformerFactory,
                                                             embeddedDBProperties.getSnapshot());
        first.next(second);
        return first;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStorageEngine eventStorageEngine) {
        return storageTransactionManagerFactory.createTransactionManager(eventStorageEngine);
    }

    @Override
    public boolean exists(String context) {
        String folder = embeddedDBProperties.getEvent().getStorage(context);
        File events = new File(folder);
        if (!events.isDirectory()) {
            return false;
        }

        String[] files = events.list((dir, name) -> name.endsWith(embeddedDBProperties.getEvent().getEventsSuffix()));
        return files != null && files.length > 0;
    }
}
