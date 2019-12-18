package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.PrimaryEventStore;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

/**
 * @author Marc Gathier
 */
public class DatafileEventStoreFactory implements EventStoreFactory {
    protected final EmbeddedDBProperties embeddedDBProperties;
    protected final MultiContextEventTransformerFactory eventTransformerFactory;

    public DatafileEventStoreFactory(EmbeddedDBProperties embeddedDBProperties,
                                     MultiContextEventTransformerFactory eventTransformerFactory) {
        this.embeddedDBProperties = embeddedDBProperties;
        this.eventTransformerFactory = eventTransformerFactory;
    }

    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        EventTransformerFactory contextEventTransformerFactory = eventTransformerFactory.factoryForContext(context);
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getEvent());
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT),
                                                        indexManager,
                                                        contextEventTransformerFactory,
                                                        embeddedDBProperties.getEvent());
        SecondaryEventStore second = new SecondaryEventStore(new EventTypeContext(context, EventType.EVENT),
                                                             indexManager,
                                                             contextEventTransformerFactory,
                                                             embeddedDBProperties.getEvent());
        first.next(second);
        return first;
    }

    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        EventTransformerFactory contextEventTransformerFactory = eventTransformerFactory.factoryForContext(context);
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getSnapshot());
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT),
                                                        indexManager,
                                                        contextEventTransformerFactory,
                                                        embeddedDBProperties.getSnapshot());
        SecondaryEventStore second = new SecondaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT),
                                                             indexManager,
                                                             contextEventTransformerFactory,
                                                             embeddedDBProperties.getSnapshot());
        first.next(second);
        return first;
    }

}
