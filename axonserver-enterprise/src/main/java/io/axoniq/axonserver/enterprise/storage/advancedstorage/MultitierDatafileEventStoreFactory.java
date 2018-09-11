package io.axoniq.axonserver.enterprise.storage.advancedstorage;

import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.SegmentBasedEventStore;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

/**
 * Author: marc
 */
public class MultitierDatafileEventStoreFactory extends DatafileEventStoreFactory {

    private final AdvancedStorageProperties advancedStorageProperties;

    public MultitierDatafileEventStoreFactory(EmbeddedDBProperties embeddedDBProperties,
                                              EventTransformerFactory eventTransformerFactory,
                                              StorageTransactionManagerFactory storageTransactionManagerFactory,
                                              AdvancedStorageProperties advancedStorageProperties) {
        super(embeddedDBProperties, eventTransformerFactory, storageTransactionManagerFactory);
        this.advancedStorageProperties = advancedStorageProperties;
    }

    @Override
    public EventStore createEventManagerChain(String context) {
        SegmentBasedEventStore base = (SegmentBasedEventStore) super.createEventManagerChain(context);
        if( advancedStorageProperties.getEventSecondary().getStorage() != null) {
            IndexManager altIndexManager = new IndexManager(context, advancedStorageProperties.getEventSecondary());
            AlternateLocationEventStore third = new AlternateLocationEventStore(new EventTypeContext(context, EventType.EVENT), altIndexManager,
                                                                                eventTransformerFactory, embeddedDBProperties.getEvent(),
                                                                                advancedStorageProperties.getEventSecondary());

            base.next(third);
        }
        return base;
    }
}
