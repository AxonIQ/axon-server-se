/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.FileSystemMonitor;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.MeterFactory;

/**
 * Factory to create event storage engines for the event store and the snapshot store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class StandardEventStoreFactory implements EventStoreFactory {

    protected final EmbeddedDBProperties embeddedDBProperties;
    protected final EventTransformerFactory eventTransformerFactory;
    protected final StorageTransactionManagerFactory storageTransactionManagerFactory;
    private final MeterFactory meterFactory;
    private final FileSystemMonitor fileSystemMonitor;
    /**
     * @param embeddedDBProperties properties for the storage engines
     * @param eventTransformerFactory transformer factory that can be used to transform events before they are stored
     * @param storageTransactionManagerFactory factory to create a transaction manager for the storage engine
     * @param meterFactory factory to create metrics meters
     * @param fileSystemMonitor
     */
    public StandardEventStoreFactory(EmbeddedDBProperties embeddedDBProperties,
                                     EventTransformerFactory eventTransformerFactory,
                                     StorageTransactionManagerFactory storageTransactionManagerFactory,
                                     MeterFactory meterFactory, FileSystemMonitor fileSystemMonitor) {
        this.embeddedDBProperties = embeddedDBProperties;
        this.eventTransformerFactory = eventTransformerFactory;
        this.storageTransactionManagerFactory = storageTransactionManagerFactory;
        this.meterFactory = meterFactory;
        this.fileSystemMonitor = fileSystemMonitor;
    }

    /**
     * Creates a storage engine for the event store for a context.
     * @param context the context
     * @return the storage engine
     */
    @Override
    public EventStorageEngine createEventStorageEngine(String context) {
        StandardIndexManager indexManager = new StandardIndexManager(context, embeddedDBProperties.getEvent(),
                                                                     EventType.EVENT,
                                                                     meterFactory);
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.EVENT),
                                                                 indexManager,
                                                                 eventTransformerFactory,
                                                                 embeddedDBProperties.getEvent(),
                                                                 meterFactory);
        return new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT),
                                     indexManager,
                                     eventTransformerFactory,
                                     embeddedDBProperties.getEvent(),
                                     second,
                                     meterFactory, fileSystemMonitor);
    }

    /**
     * Creates a storage engine for the snapshot store for a context.
     * @param context the context
     * @return the storage engine
     */
    @Override
    public EventStorageEngine createSnapshotStorageEngine(String context) {
        StandardIndexManager indexManager = new StandardIndexManager(context, embeddedDBProperties.getSnapshot(),
                                                                     EventType.SNAPSHOT,
                                                                     meterFactory);
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.SNAPSHOT),
                                                                 indexManager,
                                                                 eventTransformerFactory,
                                                                 embeddedDBProperties.getSnapshot(), meterFactory);
        return new PrimaryEventStore(new EventTypeContext(context, EventType.SNAPSHOT),
                                     indexManager,
                                     eventTransformerFactory,
                                     embeddedDBProperties.getSnapshot(), second, meterFactory, fileSystemMonitor);
    }
}
