package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.PrimaryEventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

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
    public EventStore createEventManagerChain(String context) {
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getEvent());
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT), indexManager, eventTransformerFactory, embeddedDBProperties.getEvent());
        SecondaryEventStore second = new SecondaryEventStore(new EventTypeContext(context, EventType.EVENT), indexManager,
                                                             eventTransformerFactory,
                                                             embeddedDBProperties.getEvent());
        first.next(second);
        return first;
    }

    @Override
    public EventStore createSnapshotManagerChain(String context) {
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getSnapshot());
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT), indexManager, eventTransformerFactory, embeddedDBProperties.getSnapshot());
        SecondaryEventStore second = new SecondaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT), indexManager,
                                                             eventTransformerFactory,
                                                             embeddedDBProperties.getSnapshot());
        first.next(second);
        return first;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        return storageTransactionManagerFactory.createTransactionManager(eventStore);
    }
}
