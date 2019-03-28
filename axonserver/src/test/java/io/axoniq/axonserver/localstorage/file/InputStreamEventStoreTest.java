/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.localstorage.EventInformation;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.TestUtils;
import org.junit.*;

import java.io.IOException;
import java.util.SortedSet;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Author: marc
 */
public class InputStreamEventStoreTest {
    private InputStreamEventStore testSubject;

    @Before
    public void setUp() throws IOException {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {});
        embeddedDBProperties.getEvent().setStorage(TestUtils
                                                           .fixPathOnWindows(InputStreamEventStore.class.getResource("/data").getFile()));
        String context = Topology.DEFAULT_CONTEXT;
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getEvent());
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        testSubject = new InputStreamEventStore(new EventTypeContext(context, EventType.EVENT), indexManager,
                                                                 eventTransformerFactory,
                                                                 embeddedDBProperties.getEvent());
        testSubject.init(true);
    }




    @Test
    public void getEventSource() {
        EventSource eventSource = testSubject.getEventSource(0).get();
        EventIterator iterator = eventSource.createEventIterator(0, 5);
        assertTrue(iterator.hasNext());
        EventInformation next = iterator.next();
        assertEquals(5, next.getToken());
        while( iterator.hasNext()) {
            next = iterator.next();
        }
        assertEquals(13, next.getToken());
    }

    @Test
    public void iterateTransactions() {
        EventSource eventSource = testSubject.getEventSource(0).get();
        TransactionIterator iterator = eventSource.createTransactionIterator(0, 5, true);
        assertTrue(iterator.hasNext());
        SerializedTransactionWithToken next = iterator.next();
        assertEquals(5, next.getToken());
        while( iterator.hasNext()) {
            next = iterator.next();
        }
        assertEquals(13, next.getToken());
    }

    @Test
    public void getSegments() {
        SortedSet<Long> segments = testSubject.getSegments();
        assertTrue(segments.contains(0L));
        assertTrue(segments.contains(14L));
        assertEquals(14, (long)segments.first());
    }

    @Test
    public void getAggregatePositions() {
        SortedSet<PositionInfo> positions = testSubject.getPositions(0, "a83e55b8-68ac-4287-bd9f-e9b90e5bb55c");
        assertEquals(1, positions.size());
    }
}