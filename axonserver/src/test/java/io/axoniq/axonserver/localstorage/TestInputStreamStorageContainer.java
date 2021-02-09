/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.config.FileSystemMonitor;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.StandardEventStoreFactory;
import io.axoniq.axonserver.localstorage.file.SegmentBasedEventStore;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

/**
 * @author Marc Gathier
 */
public class TestInputStreamStorageContainer {

    private final EventStorageEngine datafileManagerChain;
    private final EventStorageEngine snapshotManagerChain;
    private EventWriteStorage eventWriter;

    private FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);


    public TestInputStreamStorageContainer(File location) throws IOException {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
        });
        embeddedDBProperties.getEvent().setStorage(location.getAbsolutePath());
        embeddedDBProperties.getEvent().setSegmentSize(256 * 1024L);
        embeddedDBProperties.getEvent().setForceInterval(10000);
        embeddedDBProperties.getSnapshot().setStorage(location.getAbsolutePath());
        embeddedDBProperties.getSnapshot().setSegmentSize(512 * 1024L);
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

        doNothing().when(fileSystemMonitor).registerPath(any());

        EventStoreFactory eventStoreFactory = new StandardEventStoreFactory(embeddedDBProperties,
                                                                            new DefaultEventTransformerFactory(),
                                                                            new DefaultStorageTransactionManagerFactory(),
                                                                            meterFactory, fileSystemMonitor);
        datafileManagerChain = eventStoreFactory.createEventStorageEngine("default");
        datafileManagerChain.init(false);
        snapshotManagerChain = eventStoreFactory.createSnapshotStorageEngine("default");
        snapshotManagerChain.init(false);
        eventWriter = new EventWriteStorage(new SingleInstanceTransactionManager(datafileManagerChain));
    }


    public void createDummyEvents(int transactions, int transactionSize) {
        createDummyEvents(transactions, transactionSize, "");
    }
    public void createDummyEvents(int transactions, int transactionSize, String prefix) {
        CountDownLatch countDownLatch = new CountDownLatch(transactions);
        IntStream.range(0, transactions).parallel().forEach(j -> {
            String aggId = prefix + j;
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, transactionSize).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(
                                SerializedObject
                                        .newBuilder().build()).build());
            });
            eventWriter.store(newEvents).whenComplete((r,t) -> countDownLatch.countDown());
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public EventStorageEngine getDatafileManagerChain() {
        return datafileManagerChain;
    }

    public EventStorageEngine getSnapshotManagerChain() {
        return snapshotManagerChain;
    }

    public EventWriteStorage getEventWriter() {
        return eventWriter;
    }

    public StorageTransactionManager getTransactionManager(EventStorageEngine datafileManagerChain) {
        return new SingleInstanceTransactionManager(datafileManagerChain);
    }

    public SegmentBasedEventStore getPrimary() {
        return (SegmentBasedEventStore)datafileManagerChain;
    }

    public void close() {
        datafileManagerChain.close(false);
        snapshotManagerChain.close(false);
    }
}
