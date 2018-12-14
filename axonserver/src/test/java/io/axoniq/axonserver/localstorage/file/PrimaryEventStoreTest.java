package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.TransactionInformation;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import org.junit.*;
import org.junit.rules.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
    public void transactionsIterator() throws InterruptedException {
        setupEvents(1000, 1000);
        Iterator<TransactionWithToken> transactionWithTokenIterator = testSubject.transactionIterator(0);

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

        testSubject.initSegments(Long.MAX_VALUE);
        assertEquals(2, testSubject.getLastToken());
    }

    private void setupEvents(int numOfTransactions, int numOfEvents) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(numOfTransactions);
        IntStream.range(0, numOfTransactions).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, numOfEvents).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build());
            });
            TransactionInformation transactionInformation = new TransactionInformation(j);
            PreparedTransaction preparedTransaction = testSubject.prepareTransaction(transactionInformation, newEvents);
            testSubject.store(preparedTransaction).thenAccept(t -> latch.countDown());
        });

        latch.await(5, TimeUnit.SECONDS);
    }

}