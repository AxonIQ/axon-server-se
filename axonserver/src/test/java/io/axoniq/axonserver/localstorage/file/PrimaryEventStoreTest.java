/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
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
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.rules.*;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.unit.DataSize;
import reactor.test.StepVerifier;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * @author Marc Gathier
 */
public class PrimaryEventStoreTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private final String context = "junit";
    private final EmbeddedDBProperties embeddedDBProperties;
    private final MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

    public PrimaryEventStoreTest() {
        embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
        });
        embeddedDBProperties.getEvent().setStorage(
                tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID());
        embeddedDBProperties.getEvent().setSegmentSize(DataSize.ofKilobytes(512));
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        embeddedDBProperties.getEvent().setPrimaryCleanupDelay(0);
    }

    private final FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);

    private PrimaryEventStore primaryEventStore() {
        return primaryEventStore(EventType.EVENT);
    }

    private PrimaryEventStore primaryEventStore(EventType eventType) {
        return primaryEventStore(new StandardIndexManager(context, embeddedDBProperties::getEvent,
                                                          eventType,
                                                          meterFactory));
    }

    private PrimaryEventStore primaryEventStore(IndexManager indexManager) {
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.EVENT),
                                                                 indexManager,
                                                                 eventTransformerFactory,
                                                                 embeddedDBProperties::getEvent,
                                                                 meterFactory);

        doNothing().when(fileSystemMonitor).registerPath(any(), any());

        PrimaryEventStore testSubject = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT),
                                                              indexManager,
                                                              eventTransformerFactory,
                                                              embeddedDBProperties::getEvent,
                                                              second,
                                                              meterFactory, fileSystemMonitor);
        testSubject.init(false);
        verify(fileSystemMonitor).registerPath(any(String.class), any(Path.class));
        return testSubject;
    }

    @Test
    public void aggregateEvents() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(6);
        String aggregateId = "aggregateId";
        Event.Builder builder = Event.newBuilder()
                                     .setAggregateIdentifier(aggregateId)
                                     .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build());
        PrimaryEventStore testSubject = primaryEventStore(EventType.SNAPSHOT);

        testSubject.store(singletonList(builder.setAggregateSequenceNumber(5).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setAggregateSequenceNumber(6).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setAggregateSequenceNumber(8).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setAggregateSequenceNumber(11).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setAggregateSequenceNumber(30).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setAggregateSequenceNumber(32).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setAggregateSequenceNumber(44).build())).thenAccept(t -> latch
                .countDown());

        latch.await();
        StepVerifier.create(testSubject.eventsPerAggregate(aggregateId, 7, 20, 0)
                                       .map(SerializedEvent::getAggregateSequenceNumber))
                    .expectNext(8L, 11L)
                    .verifyComplete();

        StepVerifier.create(testSubject.eventsPerAggregate(aggregateId, 8, 30, 0)
                                       .map(SerializedEvent::getAggregateSequenceNumber))
                    .expectNext(8L, 11L)
                    .verifyComplete();


        StepVerifier.create(testSubject.eventsPerAggregate(aggregateId, 7, 30, 0)
                                       .map(SerializedEvent::getAggregateSequenceNumber))
                    .expectNext(8L, 11L)
                    .verifyComplete();

        StepVerifier.create(testSubject.eventsPerAggregate(aggregateId, 8, 31, 0)
                                       .map(SerializedEvent::getAggregateSequenceNumber))
                    .expectNext(8L, 11L, 30L)
                    .verifyComplete();
    }

    @Test
    public void testMetricsInReadingAggregatesEvents() {
        //TODO
    }

    @Test
    public void testLargeEvent() {
        PrimaryEventStore testSubject = primaryEventStore();
        storeEvent(testSubject, embeddedDBProperties.getEvent().getSegmentSize() + 1);
        storeEvent(testSubject, 10);
        long counter = 0;
        try (CloseableIterator<SerializedTransactionWithToken> transactionWithTokenIterator = testSubject
                .transactionIterator(0, Long.MAX_VALUE)) {
            while (transactionWithTokenIterator.hasNext()) {
                counter++;
                transactionWithTokenIterator.next();
            }
        }
        assertEquals(2, counter);
    }

    @Test
    public void testLargeSecondEvent() {
        PrimaryEventStore testSubject = primaryEventStore();
        storeEvent(testSubject, 10);
        storeEvent(testSubject, embeddedDBProperties.getEvent().getSegmentSize() + 1);
        long counter = 0;
        try (CloseableIterator<SerializedTransactionWithToken> transactionWithTokenIterator = testSubject
                .transactionIterator(0, Long.MAX_VALUE)) {
            while (transactionWithTokenIterator.hasNext()) {
                counter++;
                transactionWithTokenIterator.next();
            }
        }
        assertEquals(2, counter);
    }

    @Test
    public void aggregateEventsReusedAggregateIdentifier() throws InterruptedException {
        PrimaryEventStore testSubject = primaryEventStore();
        setupEvents(testSubject, 10000, 20);
        setupEvents(testSubject, 1, 5);

        List<SerializedEvent> events = testSubject.eventsPerAggregate("aggregate-0", 5, Long.MAX_VALUE, 0)
                                                  .collectList().block();
        assertNotNull(events);
        assertEquals(0, events.size());
    }

    @Test
    public void transactionsIterator() throws InterruptedException {
        PrimaryEventStore testSubject = primaryEventStore();
        setupEvents(testSubject, 1000, 2);
        Iterator<SerializedTransactionWithToken> transactionWithTokenIterator = testSubject.transactionIterator(0,
                                                                                                                Long.MAX_VALUE);

        long counter = 0;
        while (transactionWithTokenIterator.hasNext()) {
            counter++;
            transactionWithTokenIterator.next();
        }

        assertEquals(1000, counter);
    }

    @Test
    public void largeTransactions() throws InterruptedException {
        PrimaryEventStore testSubject = primaryEventStore();
        setupEvents(testSubject, 10, Short.MAX_VALUE + 5);
        Iterator<SerializedTransactionWithToken> transactionWithTokenIterator =
                testSubject.transactionIterator(0, Long.MAX_VALUE);

        long counter = 0;
        while (transactionWithTokenIterator.hasNext()) {
            counter++;
            transactionWithTokenIterator.next();
        }

        assertEquals(20, counter);
        try (CloseableIterator<SerializedEventWithToken> iterator = testSubject.getGlobalIterator(0)) {
            counter = 0;
            while (iterator.hasNext()) {
                counter++;
                iterator.next();
            }
            assertEquals(10 * (Short.MAX_VALUE + 5), counter);
        }

        List<SerializedEvent> events = testSubject.eventsPerAggregate("Aggregate-4",
                                                                      0,
                                                                            Long.MAX_VALUE,
                                                                            0)
                                                        .collect(Collectors.toList())
                .block();
        assertEquals(Short.MAX_VALUE + 5, events.size());
    }

    @Test
    public void readClosedIterator() throws InterruptedException {
        PrimaryEventStore testSubject = primaryEventStore();
        setupEvents(testSubject, 1000, 20);
        assertWithin(5, TimeUnit.SECONDS, () -> assertEquals(1, testSubject.activeSegmentCount()));
        CloseableIterator<SerializedEventWithToken> transactionWithTokenIterator = testSubject.getGlobalIterator(0);
        transactionWithTokenIterator.close();
        try {
            transactionWithTokenIterator.hasNext();
            fail("Cannot read from a closed iterator");
        } catch (IllegalStateException expected) {
            // Expected exception
        }
    }

    private void setupEvents(PrimaryEventStore testSubject, int numOfTransactions, int numOfEvents)
            throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(numOfTransactions);
        IntStream.range(0, numOfTransactions).forEach(j -> {
            String aggId = "Aggregate-" + j;
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, numOfEvents)
                     .forEach(i ->
                                      newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId)
                                                         .setAggregateSequenceNumber(i)
                                                         .setAggregateType("Demo")
                                                         .setPayload(SerializedObject.newBuilder().build()).build()));
            testSubject.store(newEvents).thenAccept(t -> latch.countDown());
        });

        if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("Timeout waiting for event store to complete");
        }
    }

    private void storeEvent(PrimaryEventStore testSubject, long payloadSize) {
        CountDownLatch latch = new CountDownLatch(1);
        byte[] buffer = new byte[(int) payloadSize];
        Arrays.fill(buffer, (byte) 'a');
        Event newEvent = Event.newBuilder().setAggregateIdentifier("11111").setAggregateSequenceNumber(0)
                              .setAggregateType("Demo").setPayload(SerializedObject.newBuilder()
                                                                                   .setData(ByteString.copyFrom(buffer))
                                                                                   .build()).build();
        testSubject.store(singletonList(newEvent)).thenAccept(t -> latch.countDown());
    }

    @Test
    public void testGlobalIterator() throws InterruptedException {
        PrimaryEventStore testSubject = primaryEventStore();
        CountDownLatch latch = new CountDownLatch(100);
        // setup with 10,000 events
        IntStream.range(0, 100).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, 100)
                     .forEach(i -> newEvents.add(Event.newBuilder()
                                                      .setAggregateIdentifier(aggId)
                                                      .setAggregateSequenceNumber(i)
                                                      .setAggregateType("Demo")
                                                      .setPayload(SerializedObject.getDefaultInstance())
                                                      .build()));
            testSubject.store(newEvents).thenAccept(t -> latch.countDown());
        });

        if (!latch.await(5, TimeUnit.SECONDS) ) {
            throw new RuntimeException("Timeout initializing event store");
        }
        try (CloseableIterator<SerializedEventWithToken> iterator = testSubject
                .getGlobalIterator(0)) {
            SerializedEventWithToken serializedEventWithToken = null;
            while (iterator.hasNext()) {
                serializedEventWithToken = iterator.next();
            }

            assertNotNull(serializedEventWithToken);
            assertEquals(9999, serializedEventWithToken.getToken());
        }
    }
}
