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
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.rules.*;
import org.springframework.data.util.CloseableIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


/**
 * @author Marc Gathier
 */
public class PrimaryEventStoreTest {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private PrimaryEventStore testSubject;

    private FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);

    @Before
    public void setUp() throws IOException {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
        });
        embeddedDBProperties.getEvent().setStorage(
                tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        embeddedDBProperties.getEvent().setSegmentSize(512 * 1024L);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        embeddedDBProperties.getEvent().setPrimaryCleanupDelay(0);
        String context = "junit";
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());
        StandardIndexManager indexManager = new StandardIndexManager(context, embeddedDBProperties.getEvent(),
                                                                     EventType.EVENT,
                                                                     meterFactory);
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.EVENT),
                                                                 indexManager,
                                                                 eventTransformerFactory,
                                                                 embeddedDBProperties.getEvent(),
                                                                 meterFactory);

        doNothing().when(fileSystemMonitor).registerPath(any());

        testSubject = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT),
                                            indexManager,
                                            eventTransformerFactory,
                                            embeddedDBProperties.getEvent(),
                                            second,
                                            meterFactory, fileSystemMonitor);
        testSubject.init(false);
        verify(fileSystemMonitor).registerPath(any(Path.class));
    }

    @Test
    public void transactionsIterator() throws InterruptedException {
        setupEvents(1000, 2);
        Iterator<SerializedTransactionWithToken> transactionWithTokenIterator = testSubject.transactionIterator(0, Long.MAX_VALUE);

        long counter = 0;
        while(transactionWithTokenIterator.hasNext()) {
            counter++;
            transactionWithTokenIterator.next();
        }

        assertEquals(1000, counter);
    }

    @Test
    public void rollbackDeleteSegments() throws InterruptedException {
        setupEvents(100, 100);
        Thread.sleep(1500);
        testSubject.rollback(9899);
        assertEquals(9899, testSubject.getLastToken());

        testSubject.rollback(859);
        assertEquals(899, testSubject.getLastToken());
    }

    @Test
    public void rollbackAndRead() throws InterruptedException {
        setupEvents(5, 3);
        Thread.sleep(1500);
        testSubject.rollback(2);
        assertEquals(2, testSubject.getLastToken());

        testSubject.initSegments(Long.MAX_VALUE, 0L);
        assertEquals(2, testSubject.getLastToken());

        storeEvent();

        testSubject.initSegments(Long.MAX_VALUE, 0L);
        assertEquals(3, testSubject.getLastToken());
    }

    private void setupEvents(int numOfTransactions, int numOfEvents) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(numOfTransactions);
        IntStream.range(0, numOfTransactions).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, numOfEvents).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId)
                                   .setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo")
                                   .setPayload(SerializedObject.newBuilder().build()).build());
            });
            testSubject.store(newEvents).thenAccept(t -> latch.countDown());
        });

        latch.await(5, TimeUnit.SECONDS);
    }

    private void storeEvent() {
        CountDownLatch latch = new CountDownLatch(1);
        Event newEvent = Event.newBuilder().setAggregateIdentifier("11111").setAggregateSequenceNumber(0)
                              .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(newEvent)).thenAccept(t -> latch.countDown());
    }

    @Test
    public void testGlobalIterator() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(100);
        // setup with 10,000 events
        IntStream.range(0, 100).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, 100).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build());
            });
            testSubject.store(newEvents).thenAccept(t -> latch.countDown());
        });

        latch.await(5, TimeUnit.SECONDS);
        try (CloseableIterator<SerializedEventWithToken> iterator = testSubject
                .getGlobalIterator(0)) {
            SerializedEventWithToken serializedEventWithToken = null;
            while(iterator.hasNext()) {
                serializedEventWithToken = iterator.next();
            }

            assertNotNull(serializedEventWithToken);
            assertEquals(9999, serializedEventWithToken.getToken());

        }
    }

    @Test
    public void testDeletingAllEvents() throws InterruptedException {
        setupEvents(5, 3);
        Thread.sleep(1500);
        testSubject.deleteAllEventData();
        assertEquals(-1, testSubject.getLastToken());

        storeEvent();
        assertEquals(0,testSubject.getLastToken());
    }
}
