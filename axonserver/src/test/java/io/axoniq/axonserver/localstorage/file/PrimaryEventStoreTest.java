package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import org.junit.*;
import org.junit.rules.*;
import org.springframework.data.util.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.hibernate.validator.internal.util.Contracts.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Author: marc
 */
public class PrimaryEventStoreTest {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private PrimaryEventStore testSubject;

    @Before
    public void setUp() throws IOException {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties();
        embeddedDBProperties.getEvent().setStorage(tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        embeddedDBProperties.getEvent().setSegmentSize(512 * 1024L);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        embeddedDBProperties.getEvent().setPrimaryCleanupDelay(0);
        String context = "junit";
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getEvent());
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        testSubject = new PrimaryEventStore(new EventTypeContext(context, EventType.EVENT), indexManager, eventTransformerFactory, embeddedDBProperties.getEvent());
        InputStreamEventStore second = new InputStreamEventStore(new EventTypeContext(context, EventType.EVENT), indexManager,
                                                             eventTransformerFactory,
                                                             embeddedDBProperties.getEvent());
        testSubject.next(second);
        testSubject.init(false);
    }


    @Test
    public void rollbackDeleteSegments() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(100);
        // setup with 10,000 events
        IntStream.range(0, 100).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<SerializedEvent> newEvents = new ArrayList<>();
            IntStream.range(0, 100).forEach(i -> {
                newEvents.add(new SerializedEvent(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build()));
            });
                PreparedTransaction preparedTransaction = testSubject.prepareTransaction(newEvents);
                testSubject.store(preparedTransaction).thenAccept(t -> latch.countDown());
        });

        latch.await(5, TimeUnit.SECONDS);
        Thread.sleep(1500);
        testSubject.rollback(9899);
        assertEquals(9899, testSubject.getLastToken());

        testSubject.rollback(859);
        assertEquals(899, testSubject.getLastToken());
    }

    @Test
    public void rollbackAndRead() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(5);
        IntStream.range(0, 5).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<SerializedEvent> newEvents = new ArrayList<>();
            IntStream.range(0, 3).forEach(i -> {
                newEvents.add(new SerializedEvent(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                                       .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build()));
            });
            PreparedTransaction preparedTransaction = testSubject.prepareTransaction(newEvents);
            testSubject.store(preparedTransaction).thenAccept(t -> latch.countDown());
        });

        latch.await(5, TimeUnit.SECONDS);
        Thread.sleep(1500);
        testSubject.rollback(2);
        assertEquals(2, testSubject.getLastToken());

        testSubject.initSegments(Long.MAX_VALUE);
        assertEquals(2, testSubject.getLastToken());
    }

    @Test
    public void testGlobalIterator() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(100);
        // setup with 10,000 events
        IntStream.range(0, 100).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<SerializedEvent> newEvents = new ArrayList<>();
            IntStream.range(0, 100).forEach(i -> {
                newEvents.add(new SerializedEvent(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                                       .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build()));
            });
            PreparedTransaction preparedTransaction = testSubject.prepareTransaction(newEvents);
            testSubject.store(preparedTransaction).thenAccept(t -> latch.countDown());
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

}