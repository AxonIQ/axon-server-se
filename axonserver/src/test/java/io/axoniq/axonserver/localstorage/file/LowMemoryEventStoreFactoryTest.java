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
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
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

/**
 * @author Marc Gathier
 */
public class LowMemoryEventStoreFactoryTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private LowMemoryEventStoreFactory testSubject;


    @Before
    public void setUp() throws Exception {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
        });
        embeddedDBProperties.getEvent().setStorage(
                tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        embeddedDBProperties.getEvent().setSegmentSize(10 * 1024L);
        embeddedDBProperties.getEvent().setPrimaryCleanupDelay(0);
        embeddedDBProperties.getEvent().setSecondaryCleanupDelay(0);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        embeddedDBProperties.getSnapshot().setSegmentSize(10 * 1024L);
        embeddedDBProperties.getSnapshot().setPrimaryCleanupDelay(0);
        embeddedDBProperties.getSnapshot().setSecondaryCleanupDelay(0);
        testSubject = new LowMemoryEventStoreFactory(embeddedDBProperties,
                                                     new DefaultEventTransformerFactory(),
                                                     new DefaultStorageTransactionManagerFactory());
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

    private SerializedEvent newEvent(String aggregateIdentifier, int sequenceNumber) {
        return new SerializedEvent(Event.newBuilder()
                                        .setAggregateIdentifier(aggregateIdentifier)
                                        .setAggregateSequenceNumber(sequenceNumber)
                                        .setAggregateType("Demo")
                                        .setPayload(
                                                SerializedObject.newBuilder()
                                                                .setData(dummyData(500))
                                                                .build())
                                        .build());
    }

    private ByteString dummyData(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'a');
        return ByteString.copyFrom(data);
    }
}
