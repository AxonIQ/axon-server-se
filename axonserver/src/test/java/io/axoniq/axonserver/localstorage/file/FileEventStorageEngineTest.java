/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.axoniq.axonserver.config.FileSystemMonitor;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.QueryOptions;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.unit.DataSize;
import reactor.test.StepVerifier;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * @author Marc Gathier
 */
public class FileEventStorageEngineTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private final String context = "junit";
    private final EmbeddedDBProperties embeddedDBProperties;
    private final MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

    public FileEventStorageEngineTest() {
        embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
        });
        embeddedDBProperties.getEvent().setStorage(
                tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID());
        embeddedDBProperties.getEvent().setSegmentSize(DataSize.ofKilobytes(512));
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        embeddedDBProperties.getEvent().setPrimaryCleanupDelay(0);
        embeddedDBProperties.getEvent().setSecondaryCleanupDelay(10);
    }

    private final FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);

    private FileEventStorageEngine primaryEventStore() {
        return primaryEventStore(EventType.EVENT);
    }

    private FileEventStorageEngine primaryEventStore(EventType eventType) {
        return primaryEventStore(new StandardIndexManager(context, embeddedDBProperties::getEvent,
                                                          embeddedDBProperties.getEvent().getPrimaryStorage(context),
                                                          eventType,
                                                          meterFactory, () -> null));
    }

    private FileEventStorageEngine primaryEventStore(IndexManager indexManager) {
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        InputStreamStrorageTierEventStore second = new InputStreamStrorageTierEventStore(new EventTypeContext(context,
                                                                                                              EventType.EVENT),
                                                                                         indexManager,
                                                                                         eventTransformerFactory,
                                                                                         embeddedDBProperties::getEvent,
                                                                                         meterFactory,
                                                                                         embeddedDBProperties.getEvent()
                                                                                                             .getPrimaryStorage(
                                                                                                                     context));

        doNothing().when(fileSystemMonitor).registerPath(any(), any());

        FileEventStorageEngine testSubject = new FileEventStorageEngine(new EventTypeContext(context, EventType.EVENT),
                                                                        indexManager,
                                                                        eventTransformerFactory,
                                                                        embeddedDBProperties::getEvent,
                                                                        () -> second,
                                                                        meterFactory,
                                                                        fileSystemMonitor,
                                                                        embeddedDBProperties.getEvent()
                                                                                            .getPrimaryStorage(context));
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
        FileEventStorageEngine testSubject = primaryEventStore(EventType.SNAPSHOT);

        testSubject.store(singletonList(builder.setMessageIdentifier("1").setAggregateSequenceNumber(5).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setMessageIdentifier("1").setAggregateSequenceNumber(6).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setMessageIdentifier("1").setAggregateSequenceNumber(8).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setMessageIdentifier("1").setAggregateSequenceNumber(11).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setMessageIdentifier("1").setAggregateSequenceNumber(30).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setMessageIdentifier("1").setAggregateSequenceNumber(32).build())).thenAccept(t -> latch
                .countDown());
        testSubject.store(singletonList(builder.setMessageIdentifier("1").setAggregateSequenceNumber(44).build())).thenAccept(t -> latch
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
    public void testLargeEvent() throws ExecutionException, InterruptedException, TimeoutException {
        FileEventStorageEngine testSubject = primaryEventStore();
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
    public void queryEvents() throws ExecutionException, InterruptedException {
        FileEventStorageEngine testSubject = primaryEventStore();
        for (int i = 0; i < 1000; i++) {
            byte[] buffer = new byte[200];
            Arrays.fill(buffer, (byte) 'x');
            Event.Builder builder = Event.newBuilder()
                                         .setAggregateIdentifier("query-" + i)
                                         .setPayload(SerializedObject.newBuilder()
                                                                     .setData(ByteString.copyFrom(buffer))
                                                                     .build())
                                         .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build());
            for (int j = 0; j < 20; j++) {
                List<Event> events = new ArrayList<>();
                events.add(builder.setMessageIdentifier(UUID.randomUUID().toString())
                                  .setAggregateSequenceNumber(j)
                                  .setTimestamp(System.currentTimeMillis())
                                  .build());
                testSubject.store(events).get();
            }
        }

        // ensure all segments are closed
        assertWithin(5, TimeUnit.SECONDS, () -> assertEquals(1, testSubject.activeSegmentCount()));

        AtomicInteger counter = new AtomicInteger();
        testSubject.query(new QueryOptions(0, Long.MAX_VALUE, 0), e -> {
            counter.incrementAndGet();
            return true;
        });
        assertEquals(20_000, counter.get());
        counter.set(0);
        testSubject.query(new QueryOptions(0, 100, 0), e -> {
            counter.incrementAndGet();
            return true;
        });
        assertEquals(101, counter.get());
        counter.set(0);
        testSubject.query(new QueryOptions(500, 650, 0), e -> {
            counter.incrementAndGet();
            return true;
        });
        assertEquals(151, counter.get());
        counter.set(0);
        testSubject.query(new QueryOptions(0, Long.MAX_VALUE, 0), e -> {
            counter.incrementAndGet();
            return e.getToken() == 10_000;
        });
        assertTrue(counter.get() < 20_000);
    }

    @Test
    public void testLargeSecondEvent() throws ExecutionException, InterruptedException, TimeoutException {
        FileEventStorageEngine testSubject = primaryEventStore();
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
    public void testEventVersions() throws ExecutionException, InterruptedException, TimeoutException {
        FileEventStorageEngine testSubject = primaryEventStore();
        storeEvent(testSubject, 10);
        storeEventWithNewVersion(testSubject, 10, 1);
        long counter = 0;
        try (CloseableIterator<SerializedTransactionWithToken> transactionWithTokenIterator = testSubject
                .transactionIterator(0, Long.MAX_VALUE)) {
            while (transactionWithTokenIterator.hasNext()) {
                counter++;
                transactionWithTokenIterator.next();
            }
        }
        assertEquals(2, counter);
        assertEquals(2, testSubject.nextToken());
        File file = new File(embeddedDBProperties.getEvent().getPrimaryStorage(context));
        Set<String> expectedFilenames = Sets.newHashSet("00000000000000000000.events",
                                                        "00000000000000000001_00001.events");
        for (File f : file.listFiles()) {
            assertTrue(expectedFilenames.remove(f.getName()));
        }
        assertTrue(expectedFilenames.isEmpty());
    }

    @Test
    public void testFirstEventOtherVersion() throws ExecutionException, InterruptedException, TimeoutException {
        FileEventStorageEngine testSubject = primaryEventStore();
        storeEventWithNewVersion(testSubject, 10, 1);
        long counter = 0;
        try (CloseableIterator<SerializedTransactionWithToken> transactionWithTokenIterator = testSubject
                .transactionIterator(0, Long.MAX_VALUE)) {
            while (transactionWithTokenIterator.hasNext()) {
                counter++;
                transactionWithTokenIterator.next();
            }
        }
        assertEquals(1, counter);
        assertEquals(1, testSubject.nextToken());
        File file = new File(embeddedDBProperties.getEvent().getPrimaryStorage(context));
        Set<String> expectedFilenames = Sets.newHashSet(
                "00000000000000000000_00001.events");
        for (File f : file.listFiles()) {
            assertTrue(expectedFilenames.remove(f.getName()));
        }
        assertTrue(expectedFilenames.isEmpty());

        assertTrue(testSubject.getBackupFilenames(-1, 0, true)
                              .anyMatch(p -> p.endsWith("00000000000000000000_00001.events")));
    }

    @Test
    public void aggregateEventsReusedAggregateIdentifier() throws InterruptedException {
        FileEventStorageEngine testSubject = primaryEventStore();
        setupEvents(testSubject, 10000, 20);
        setupEvents(testSubject, 1, 5);

        List<SerializedEvent> events = testSubject.eventsPerAggregate("aggregate-0", 5, Long.MAX_VALUE, 0)
                                                  .collectList().block();
        assertNotNull(events);
        assertEquals(0, events.size());
    }

    @Test
    public void transactionsIterator() throws InterruptedException {
        FileEventStorageEngine testSubject = primaryEventStore();
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
        FileEventStorageEngine testSubject = primaryEventStore();
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
        FileEventStorageEngine testSubject = primaryEventStore();
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

    private void setupEvents(FileEventStorageEngine testSubject, int numOfTransactions, int numOfEvents)
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

    private void storeEvent(FileEventStorageEngine testSubject, long payloadSize)
            throws ExecutionException, InterruptedException, TimeoutException {
        byte[] buffer = new byte[(int) payloadSize];
        Arrays.fill(buffer, (byte) 'a');
        Event newEvent = Event.newBuilder().setAggregateIdentifier("11111").setAggregateSequenceNumber(0)
                              .setAggregateType("Demo").setPayload(SerializedObject.newBuilder()
                                                                                   .setData(ByteString.copyFrom(buffer))
                                                                                   .build()).build();
        testSubject.store(singletonList(newEvent)).get(1, TimeUnit.SECONDS);
    }

    private void storeEventWithNewVersion(FileEventStorageEngine testSubject, int payloadSize, int segmentVersion)
            throws ExecutionException, InterruptedException, TimeoutException {
        byte[] buffer = new byte[(int) payloadSize];
        Arrays.fill(buffer, (byte) 'a');
        Event newEvent = Event.newBuilder().setAggregateIdentifier("11111").setAggregateSequenceNumber(0)
                              .setAggregateType("Demo").setPayload(SerializedObject.newBuilder()
                                                                                   .setData(ByteString.copyFrom(buffer))
                                                                                   .build()).build();
        testSubject.store(singletonList(newEvent), segmentVersion).get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testGlobalIterator() throws InterruptedException {
        FileEventStorageEngine testSubject = primaryEventStore();
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

   /* @Test
    public void testTransformation() throws InterruptedException {
        PrimaryEventStore testSubject = primaryEventStore();
        try {
            CountDownLatch latch = new CountDownLatch(100);
            SerializedObject payload = SerializedObject.newBuilder()
                                                       .setData(randomData(500))
                                                       .setType("ToBeTransformed")
                                                       .build();
            // setup with 10,000 events
            IntStream.range(0, 100).forEach(j -> {
                String aggId = "Aggregate-" + j;
                List<Event> newEvents = new ArrayList<>();
                IntStream.range(0, 100).forEach(i -> {
                    newEvents.add(Event.newBuilder()
                                       .setMessageIdentifier(UUID.randomUUID().toString())
                                       .setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                       .setAggregateType("Demo").setPayload(payload).build());
                });
                testSubject.store(newEvents).thenAccept(t -> latch.countDown());
            });

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertWithin( 30, TimeUnit.SECONDS, () -> assertEquals(1, testSubject.activeSegments().size()));

            testSubject.transformContents(0, Long.MAX_VALUE, false, 1, (e, token) -> {
                if (!"Aggregate-1".equals(e.getAggregateIdentifier())) {
                    return new EventTransformationResult() {
                        @Override
                        public Event event() {
                            return e;
                        }

                        @Override
                        public long nextToken() {
                            return token + 1;
                        }
                    };
                }
                Event updated = Event.newBuilder(e).setPayload(SerializedObject.newBuilder(e.getPayload()).
                                                                               setType("Transformed")).build();

                return new EventTransformationResult() {
                    @Override
                    public Event event() {
                        return updated;
                    }

                    @Override
                    public long nextToken() {
                        return token + 1;
                    }
                };
            }, progress -> {
            });

            List<SerializedEvent> events = testSubject.eventsPerAggregate("Aggregate-1",
                                                                                0,
                                                                                Long.MAX_VALUE,
                                                                                0)
                                                            .collect(Collectors.toList())
                    .block(Duration.ofSeconds(5));
            assertEquals(100, events.size());
            events.forEach(e -> assertEquals("Transformed", e.getPayloadType()));
        } finally {
            Thread.sleep(10);
            testSubject.close(true);
        }
    }
*/
    /*@Test
    public void testTransformationCurrentSegment() throws InterruptedException {
        PrimaryEventStore testSubject = primaryEventStore();
        try {
            CountDownLatch latch = new CountDownLatch(10);
            SerializedObject payload = SerializedObject.newBuilder()
                                                       .setData(randomData(500))
                                                       .setType("ToBeTransformed")
                                                       .build();
            // setup with 1,000 events
            IntStream.range(0, 10).forEach(j -> {
                String aggId = "Aggregate-" + j;
                List<Event> newEvents = new ArrayList<>();
                IntStream.range(0, 100).forEach(i -> {
                    newEvents.add(Event.newBuilder()
                                       .setMessageIdentifier(UUID.randomUUID().toString())
                                       .setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                       .setAggregateType("Demo").setPayload(payload).build());
                });
                testSubject.store(newEvents).thenAccept(t -> latch.countDown());
            });

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertWithin( 30, TimeUnit.SECONDS, () -> assertEquals(1, testSubject.activeSegments().size()));


            testSubject.transformContents(0, Long.MAX_VALUE, false, 1, (e, token) -> {
                if (!"Aggregate-9".equals(e.getAggregateIdentifier())) {
                    return eventTransformerResult(e, token + 1);
                }

                return eventTransformerResult(Event.newBuilder(e)
                                                   .setPayload(SerializedObject.newBuilder(e.getPayload()).
                                                                               setType("Transformed")).build(),
                                              token + 1);
            }, progress -> {
            });

            List<SerializedEvent> events = testSubject.eventsPerAggregate("Aggregate-9",
                                                                          0,
                                                                          Long.MAX_VALUE,
                                                                          0)
                                                      .collect(Collectors.toList())
                                                      .block(Duration.ofSeconds(5));
            assertEquals(100, events.size());
            events.forEach(e -> assertEquals("Transformed", e.getPayloadType()));
        } finally {
            Thread.sleep(10);
            testSubject.close(true);
        }
    }*/
/*
    private EventTransformationResult eventTransformerResult(Event event, long nextToken) {
        return new EventTransformationResult() {
            @Override
            public Event event() {
                return event;
            }

            @Override
            public long nextToken() {
                return nextToken;
            }
        };
    } /*

    /*@Test
    public void testReadingDuringTransformation() throws InterruptedException, ExecutionException, TimeoutException {
        PrimaryEventStore testSubject = primaryEventStore();
        try {
            CountDownLatch latch = new CountDownLatch(10);
            SerializedObject payload = SerializedObject.newBuilder()
                                                       .setData(randomData(500))
                                                       .setType("ToBeTransformed")
                                                       .build();
            // setup with 1,000 events
            IntStream.range(0, 10).forEach(j -> {
                String aggId = "Aggregate-" + j;
                List<Event> newEvents = new ArrayList<>();
                IntStream.range(0, 100).forEach(i -> {
                    newEvents.add(Event.newBuilder()
                                       .setMessageIdentifier(UUID.randomUUID().toString())
                                       .setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                       .setAggregateType("Demo").setPayload(payload).build());
                });
                testSubject.store(newEvents).thenAccept(t -> latch.countDown());
            });

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertWithin( 30, TimeUnit.SECONDS, () -> assertEquals(1, testSubject.activeSegments().size()));

            AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
            List<SerializedEvent> events = new ArrayList<>();
            CompletableFuture<Void> completed = new CompletableFuture<>();
            testSubject.eventsPerAggregate("Aggregate-1",
                                           0,
                                           Long.MAX_VALUE,
                                           0).subscribe(new Subscriber<SerializedEvent>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscriptionRef.set(subscription);
//                    subscription.request(1);
                }

                @Override
                public void onNext(SerializedEvent serializedEvent) {
                    events.add(serializedEvent);
                }

                @Override
                public void onError(Throwable throwable) {
                    completed.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    completed.complete(null);
                }
            });


            testSubject.transformContents(0, Long.MAX_VALUE, false, 1, (e, token) -> {
                if (!"Aggregate-1".equals(e.getAggregateIdentifier())) {
                    return eventTransformerResult(e, token + 1);
                }

                return eventTransformerResult(Event.newBuilder(e)
                                                   .setPayload(SerializedObject.newBuilder(e.getPayload()).
                                                                               setType("Transformed")).build(),
                                              token + 1);
            }, progress -> {
            });

            subscriptionRef.get().request(1000);
            completed.get(5, TimeUnit.SECONDS);
            assertEquals(100, events.size());
            events.forEach(e -> assertEquals("invalid sequence nr: " + e.getAggregateSequenceNumber(),
                                             "ToBeTransformed", e.getPayloadType()));
        } finally {
            Thread.sleep(100);
            testSubject.close(true);
        }
    }*/

    private ByteString randomData(int i) {
        byte[] bytes = new byte[i];
        Arrays.fill(bytes, (byte)'x');
        return ByteString.copyFrom(bytes);
    }
}
