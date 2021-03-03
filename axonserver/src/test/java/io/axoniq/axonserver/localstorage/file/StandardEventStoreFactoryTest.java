/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;


import com.google.protobuf.ByteString;
import io.axoniq.axonserver.config.FileSystemMonitor;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.rules.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.any;

/**
 * @author Marc Gathier
 */
public class StandardEventStoreFactoryTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private final FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);

    private StandardEventStoreFactory testSubject;


    @Before
    public void setUp() throws Exception {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
        });
        embeddedDBProperties.getEvent().setStorage(
                tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        embeddedDBProperties.getEvent().setSegmentSize(10 * 1024L);
        embeddedDBProperties.getEvent().setPrimaryCleanupDelay(0);
        embeddedDBProperties.getEvent().setSecondaryCleanupDelay(0);
        embeddedDBProperties.getEvent().setUseMmapIndex(false);
        embeddedDBProperties.getEvent().setForceCleanMmapIndex(true);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        embeddedDBProperties.getSnapshot().setSegmentSize(10 * 1024L);
        embeddedDBProperties.getSnapshot().setPrimaryCleanupDelay(0);
        embeddedDBProperties.getSnapshot().setSecondaryCleanupDelay(0);
        embeddedDBProperties.getSnapshot().setUseMmapIndex(false);
        embeddedDBProperties.getSnapshot().setForceCleanMmapIndex(true);
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());
        doNothing().when(fileSystemMonitor).registerPath(any());
        testSubject = new StandardEventStoreFactory(embeddedDBProperties,
                                                    new DefaultEventTransformerFactory(),
                                                    new DefaultStorageTransactionManagerFactory(), meterFactory, fileSystemMonitor);
    }

    @Test
    public void createEventStorageEngine() throws ExecutionException, InterruptedException {
        EventStorageEngine snapshotStorageEngine = testSubject.createSnapshotStorageEngine(
                "DEMO2");
        snapshotStorageEngine.init(false);
        EventStorageEngine storageEngine = testSubject.createEventStorageEngine(
                "DEMO2");
        storageEngine.init(false);
        List<CompletableFuture<Long>> futures =
                IntStream.range(0, 100)
                         .mapToObj(index -> storageEngine
                                 .store(Collections.singletonList(newEvent("EVENT_AGGREGATE" + index, 0))))
                         .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        storageEngine.close(false);

        EventStorageEngine storageEngine2 = testSubject.createEventStorageEngine(
                "DEMO2");
        storageEngine2.init(true);
        assertEquals(0, (long) storageEngine2.getLastSequenceNumber("EVENT_AGGREGATE0").orElse(-1L));
        storageEngine2.close(true);
        snapshotStorageEngine.close(true);
    }

    @Test
    public void createSnapshotStorageEngine() throws InterruptedException, ExecutionException {
        EventStorageEngine eventStorageEngine = testSubject.createEventStorageEngine(
                "DEMO");
        eventStorageEngine.init(false);
        EventStorageEngine storageEngine = testSubject.createSnapshotStorageEngine(
                "DEMO");
        storageEngine.init(false);
        List<CompletableFuture<Long>> futures =
                IntStream.range(0, 100)
                         .mapToObj(index -> storageEngine
                                 .store(Collections.singletonList(newEvent("SNAPSHOT_AGGREGATE" + index, 0))))
                         .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        storageEngine.close(false);

        EventStorageEngine storageEngine2 = testSubject.createSnapshotStorageEngine(
                "DEMO");
        storageEngine2.init(true);
        assertEquals(0, (long) storageEngine2.getLastSequenceNumber("SNAPSHOT_AGGREGATE0").orElse(-1L));
        storageEngine2.close(true);
        eventStorageEngine.close(true);
    }

    private Event newEvent(String aggregateIdentifier, int sequenceNumber) {
        return Event.newBuilder()
                    .setAggregateIdentifier(aggregateIdentifier)
                    .setAggregateSequenceNumber(sequenceNumber)
                    .setAggregateType("Demo")
                    .setPayload(
                            SerializedObject.newBuilder()
                                            .setData(dummyData(500))
                                            .build())
                    .build();
    }

    private ByteString dummyData(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'a');
        return ByteString.copyFrom(data);
    }
}
