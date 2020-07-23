package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.enterprise.storage.file.xref.JumpSkipIndexManager;
import io.axoniq.axonserver.enterprise.storage.multitier.LowerTierAggregateSequenceNumberResolver;
import io.axoniq.axonserver.enterprise.storage.multitier.MultiTierInformationProvider;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.PrimaryEventStore;
import io.axoniq.axonserver.localstorage.file.StandardIndexManager;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.MeterFactory;

/**
 * Implementation of the {@link EventStoreFactory} for Enterprise Edition.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class DatafileEventStoreFactory implements EventStoreFactory {

    protected final EmbeddedDBPropertiesProvider embeddedDBPropertiesProvider;
    protected final MultiContextEventTransformerFactory eventTransformerFactory;
    protected final MultiTierInformationProvider multiTierInformationProvider;
    private final LowerTierAggregateSequenceNumberResolver lowerTierAggregateSequenceNumberResolver;
    private final MeterFactory meterFactory;

    public DatafileEventStoreFactory(EmbeddedDBPropertiesProvider embeddedDBPropertiesProvider,
                                     MultiContextEventTransformerFactory eventTransformerFactory,
                                     MultiTierInformationProvider multiTierInformationProvider,
                                     LowerTierAggregateSequenceNumberResolver lowerTierAggregateSequenceNumberResolver,
                                     MeterFactory meterFactory) {
        this.embeddedDBPropertiesProvider = embeddedDBPropertiesProvider;
        this.eventTransformerFactory = eventTransformerFactory;
        this.multiTierInformationProvider = multiTierInformationProvider;
        this.lowerTierAggregateSequenceNumberResolver = lowerTierAggregateSequenceNumberResolver;
        this.meterFactory = meterFactory;
    }

    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        EventTransformerFactory contextEventTransformerFactory = eventTransformerFactory.factoryForContext(context);
        StorageProperties properties = embeddedDBPropertiesProvider.getEventProperties(context);
        IndexManager indexManager;
        if (EmbeddedDBPropertiesProvider.JUMP_SKIP_INDEX.equals(properties.getIndexFormat())) {
            indexManager = new JumpSkipIndexManager(context,
                                                    properties,
                                                    EventType.EVENT,
                                                    meterFactory);
        } else {
            indexManager = new StandardIndexManager(context,
                                                    properties,
                                                    EventType.EVENT,
                                                    lowerTierAggregateSequenceNumberResolver,
                                                    meterFactory);
        }
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT),
                                                        indexManager,
                                                        contextEventTransformerFactory,
                                                        properties,
                                                        meterFactory);
        ReadOnlyEventStoreSegments second = new ReadOnlyEventStoreSegments(new EventTypeContext(context,
                                                                                                EventType.EVENT),
                                                                           indexManager,
                                                                           contextEventTransformerFactory,
                                                                           properties,
                                                                           multiTierInformationProvider,
                                                                           meterFactory);
        first.next(second);
        return first;
    }

    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        EventTransformerFactory contextEventTransformerFactory = eventTransformerFactory.factoryForContext(context);
        StorageProperties properties = embeddedDBPropertiesProvider.getSnapshotProperties(context);
        IndexManager indexManager;
        if (EmbeddedDBPropertiesProvider.JUMP_SKIP_INDEX.equals(properties.getIndexFormat())) {
            indexManager = new JumpSkipIndexManager(context,
                                                    properties,
                                                    EventType.SNAPSHOT,
                                                    meterFactory);
        } else {
            indexManager = new StandardIndexManager(context,
                                                    properties,
                                                    EventType.SNAPSHOT,
                                                    meterFactory);
        }
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT),
                                                        indexManager,
                                                        contextEventTransformerFactory,
                                                        properties,
                                                        meterFactory);
        ReadOnlyEventStoreSegments second = new ReadOnlyEventStoreSegments(new EventTypeContext(context,
                                                                                                EventType.SNAPSHOT),
                                                                           indexManager,
                                                                           contextEventTransformerFactory,
                                                                           properties,
                                                                           multiTierInformationProvider,
                                                             meterFactory);
        first.next(second);
        return first;
    }

}
