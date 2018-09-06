package io.axoniq.axonhub.localstorage.file;

import io.axoniq.axonhub.localstorage.EventStore;
import io.axoniq.axonhub.localstorage.EventStoreFactory;
import io.axoniq.axonhub.localstorage.EventType;
import io.axoniq.axonhub.localstorage.EventTypeContext;
import io.axoniq.axonhub.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonhub.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonhub.localstorage.transformation.EventTransformerFactory;

/**
 * Author: marc
 */
public class LowMemoryEventStoreFactory implements EventStoreFactory {
    protected final EmbeddedDBProperties embeddedDBProperties;
    protected final EventTransformerFactory eventTransformerFactory;
    protected final StorageTransactionManagerFactory storageTransactionManagerFactory;

    public LowMemoryEventStoreFactory(EmbeddedDBProperties embeddedDBProperties, EventTransformerFactory eventTransformerFactory,
                                      StorageTransactionManagerFactory storageTransactionManagerFactory) {
        this.embeddedDBProperties = embeddedDBProperties;
        this.eventTransformerFactory = eventTransformerFactory;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
    }

    @Override
    public EventStore createEventManagerChain(String context) {
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getEvent());
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT), indexManager, eventTransformerFactory, embeddedDBProperties.getEvent());
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.EVENT), indexManager,
                                                             eventTransformerFactory,
                                                             embeddedDBProperties.getEvent());
        first.next(second);
        return first;
    }

    @Override
    public EventStore createSnapshotManagerChain(String context) {
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getSnapshot());
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT), indexManager, eventTransformerFactory, embeddedDBProperties.getSnapshot());
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.EVENT), indexManager,
                                                                 eventTransformerFactory,
                                                                 embeddedDBProperties.getEvent());
        first.next(second);
        return first;
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStore eventStore) {
        return storageTransactionManagerFactory.createTransactionManager(eventStore);
    }
}
