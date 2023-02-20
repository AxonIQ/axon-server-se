/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.config.FileSystemMonitor;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.FileEventStorageEngine;
import io.axoniq.axonserver.localstorage.file.StandardEventStoreFactory;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.util.unit.DataSize;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
        this(location, e -> e);
    }

    public TestInputStreamStorageContainer(File location,
                                           Function<EmbeddedDBProperties, EmbeddedDBProperties> propertiesCustomizer)
            throws IOException {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
        });
        embeddedDBProperties.getEvent().setStorage(location.getAbsolutePath());
        embeddedDBProperties.getEvent().setSegmentSize(DataSize.ofKilobytes(256));
        embeddedDBProperties.getEvent().setForceInterval(10000);
        embeddedDBProperties.getSnapshot().setStorage(location.getAbsolutePath());
        embeddedDBProperties.getSnapshot().setSegmentSize(DataSize.ofKilobytes(512));
        embeddedDBProperties = propertiesCustomizer.apply(embeddedDBProperties);
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

        doNothing().when(fileSystemMonitor).registerPath(any(), any());

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
        AtomicReference<Throwable> error = new AtomicReference<>();

        IntStream.range(0, transactions).forEach(j -> {
            String aggId = prefix + j;
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, transactionSize).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(
                                SerializedObject
                                        .newBuilder().build()).build());
            });
            eventWriter.storeBatch(newEvents)
                       .doOnSuccess((s -> countDownLatch.countDown()))
                       .doOnError(t -> {
                           error.set(t);
                           countDownLatch.countDown();
                       })
                       .subscribe();
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (error.get() != null) {
            throw MessagingPlatformException.create(error.get());
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

    public FileEventStorageEngine getPrimary() {
        return (FileEventStorageEngine) datafileManagerChain;
    }

    public void close() {
        datafileManagerChain.close(false);
        snapshotManagerChain.close(false);
    }
}
