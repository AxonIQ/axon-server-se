package io.axoniq.axonhub.modules.advancedstorage;

import io.axoniq.axonhub.localstorage.EventStore;
import io.axoniq.axonhub.localstorage.EventType;
import io.axoniq.axonhub.localstorage.EventTypeContext;
import io.axoniq.axonhub.localstorage.file.DatafileEventStoreFactory;
import io.axoniq.axonhub.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonhub.localstorage.file.IndexManager;
import io.axoniq.axonhub.localstorage.file.SegmentBasedEventStore;
import io.axoniq.axonhub.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonhub.localstorage.transformation.EventTransformerFactory;

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
