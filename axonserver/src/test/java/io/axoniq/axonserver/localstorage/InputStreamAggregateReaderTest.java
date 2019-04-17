/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import org.junit.*;
import org.junit.rules.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marc Gathier
 */
public class InputStreamAggregateReaderTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static TestInputStreamStorageContainer testStorageContainer;
    private AggregateReader testSubject;

    @BeforeClass
    public static void init() throws Exception {
        testStorageContainer = new TestInputStreamStorageContainer(tempFolder.getRoot());
        testStorageContainer.createDummyEvents(1000, 100);

        SnapshotWriteStorage snapshotWriteStorage = new SnapshotWriteStorage(testStorageContainer.getTransactionManager(testStorageContainer.getSnapshotManagerChain()));
        snapshotWriteStorage.store(Event.newBuilder().setAggregateIdentifier("55").setAggregateSequenceNumber(75)
                                        .setAggregateType("Snapshot")
                                        .setPayload(SerializedObject
                                                            .newBuilder().build()).build());

    }

    @AfterClass
    public static void close() {
        testStorageContainer.close();
    }


    @Before
    public void setUp() {
        testSubject = new AggregateReader(testStorageContainer.getDatafileManagerChain(), new SnapshotReader(testStorageContainer.getSnapshotManagerChain()));
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
    public void readEventsWithSnapshot() {
        List<Event> events = new ArrayList<>();
        testSubject.readEvents("55", true, 0, serialized -> {
            events.add(serialized.asEvent());
        });

        Assert.assertEquals(25, events.size());
        Assert.assertEquals("Snapshot", events.get(0).getAggregateType());
    }

    @Test
    public void readEventsWithSnapshotBeforeMin() {
        List<Event> events = new ArrayList<>();
        testSubject.readEvents("55", true, 90, serialized -> {
            events.add(serialized.asEvent());
        });

        Assert.assertEquals(10, events.size());
        Assert.assertEquals("Demo", events.get(0).getAggregateType());
    }

}
