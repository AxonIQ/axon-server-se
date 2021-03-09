/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import org.junit.*;
import org.junit.rules.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marc Gathier
 */
public class AggregateReaderTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static TestInputStreamStorageContainer testStorageContainer;
    private AggregateReader testSubject;

    @BeforeClass
    public static void init() throws Exception {
        testStorageContainer = new TestInputStreamStorageContainer(tempFolder.getRoot());
        testStorageContainer.createDummyEvents(1000, 100);

        SnapshotWriteStorage snapshotWriteStorage = new SnapshotWriteStorage(testStorageContainer.getTransactionManager(
                testStorageContainer.getSnapshotManagerChain()));
        List<CompletableFuture<Confirmation>> completableFutures = new ArrayList<>();
        completableFutures.add(snapshotWriteStorage.store(Event.newBuilder().setAggregateIdentifier("55")
                                                               .setAggregateSequenceNumber(5)
                                                               .setAggregateType("Snapshot")
                                                               .setPayload(SerializedObject
                                                                                   .newBuilder().build()).build()));
        completableFutures.add(snapshotWriteStorage.store(Event.newBuilder().setAggregateIdentifier("55")
                                                               .setAggregateSequenceNumber(15)
                                                               .setAggregateType("Snapshot")
                                                               .setPayload(SerializedObject
                                                                                   .newBuilder().build()).build()));
        completableFutures.add(snapshotWriteStorage.store(Event.newBuilder().setAggregateIdentifier("55")
                                                               .setAggregateSequenceNumber(25)
                                                               .setAggregateType("Snapshot")
                                                               .setPayload(SerializedObject
                                                                                   .newBuilder().build()).build()));
        completableFutures.add(snapshotWriteStorage.store(Event.newBuilder().setAggregateIdentifier("55")
                                                               .setAggregateSequenceNumber(75)
                                                               .setAggregateType("Snapshot")
                                                               .setPayload(SerializedObject
                                                                                   .newBuilder().build()).build()));

        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).get(1, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void close() {
        testStorageContainer.close();
    }

    @Before
    public void setUp() {
        testSubject = new AggregateReader(testStorageContainer.getDatafileManagerChain(),
                                          new SnapshotReader(testStorageContainer.getSnapshotManagerChain()));
    }

    @Test
    public void readEventsFromOldSegment() {
        AtomicLong sequenceNumber = new AtomicLong();
        testSubject.readEvents("1", true, 0,
                               event -> sequenceNumber.set(event.getAggregateSequenceNumber()));
        Assert.assertEquals(99, sequenceNumber.intValue());
    }

    @Test
    public void readEventsFromCurrentSegment() {
        AtomicLong sequenceNumber = new AtomicLong();
        testSubject.readEvents("999", true, 0,
                               event -> sequenceNumber.set(event.getAggregateSequenceNumber()));
        Assert.assertEquals(99, sequenceNumber.intValue());
    }

    @Test
    public void readSnapshots() {
        List<Long> sequenceNumbers = new ArrayList<>();
        testSubject.readSnapshots("55", 10, 50, 10,
                                  event -> sequenceNumbers.add(event.getAggregateSequenceNumber()));
        Assert.assertEquals(2, sequenceNumbers.size());
    }

    @Test
    public void readAllSnapshots() {
        List<Long> sequenceNumbers = new ArrayList<>();
        testSubject.readSnapshots("55", 0, Long.MAX_VALUE, 0,
                                  event -> sequenceNumbers.add(event.getAggregateSequenceNumber()));
        Assert.assertEquals(4, sequenceNumbers.size());
    }

    @Test
    public void readEventsWithSnapshot() {
        List<Event> events = new ArrayList<>();
        testSubject.readEvents("55", true, 0, event -> {
            events.add(event.asEvent());
        });

        Assert.assertEquals(25, events.size());
        Assert.assertEquals("Snapshot", events.get(0).getAggregateType());
    }

    @Test
    public void readEventsWithSnapshotBeforeMin() {
        List<Event> events = new ArrayList<>();
        testSubject.readEvents("55", true, 90, event -> {
            events.add(event.asEvent());
        });

        Assert.assertEquals(10, events.size());
        Assert.assertEquals("Demo", events.get(0).getAggregateType());
    }

    @Test
    public void readEventsWithSnapshotAfterMax() {
        List<Event> events = new ArrayList<>();
        testSubject.readEvents("55", true, 0, 30, 0, event -> {
            events.add(event.asEvent());
        });

        Assert.assertEquals(5, events.size());
        Assert.assertEquals("Snapshot", events.get(0).getAggregateType());
        Assert.assertEquals(25, events.get(0).getAggregateSequenceNumber());
    }

    @Test
    public void readHighestSequenceNr() {
        Assert.assertEquals(99, testSubject.readHighestSequenceNr("55"));
    }
}