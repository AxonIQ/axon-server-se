/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.MeterFactory;

/**
 * @author Marc Gathier
 */
public class LowMemoryEventStoreFactory implements EventStoreFactory {
    protected final EmbeddedDBProperties embeddedDBProperties;
    protected final EventTransformerFactory eventTransformerFactory;
    protected final StorageTransactionManagerFactory storageTransactionManagerFactory;
    private final MeterFactory meterFactory;

    public LowMemoryEventStoreFactory(EmbeddedDBProperties embeddedDBProperties,
                                      EventTransformerFactory eventTransformerFactory,
                                      StorageTransactionManagerFactory storageTransactionManagerFactory,
                                      MeterFactory meterFactory) {
        this.embeddedDBProperties = embeddedDBProperties;
        this.eventTransformerFactory = eventTransformerFactory;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
        this.meterFactory = meterFactory;
    }

    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        StandardIndexManager indexManager = new StandardIndexManager(context, embeddedDBProperties.getEvent(),
                                                                     meterFactory);
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT),
                                                        indexManager,
                                                        eventTransformerFactory,
                                                        embeddedDBProperties.getEvent(),
                                                        meterFactory);
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.EVENT),
                                                                 indexManager,
                                                                 eventTransformerFactory,
                                                                 embeddedDBProperties.getEvent(),
                                                                 meterFactory);
        first.next(second);
        return first;
    }

    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        StandardIndexManager indexManager = new StandardIndexManager(context, embeddedDBProperties.getSnapshot(),
                                                                     meterFactory);
        PrimaryEventStore first = new PrimaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT),
                                                        indexManager,
                                                        eventTransformerFactory,
                                                        embeddedDBProperties.getSnapshot(), meterFactory);
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.SNAPSHOT),
                                                                 indexManager,
                                                                 eventTransformerFactory,
                                                                 embeddedDBProperties.getSnapshot(), meterFactory);
        first.next(second);
        return first;
    }
}
