package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axondb.Event;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.StorageCallback;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.platform.SerializedObject;
import org.junit.*;
import org.junit.rules.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

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
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, 100).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build());
            });
                PreparedTransaction preparedTransaction = testSubject.prepareTransaction(newEvents);
                testSubject.store(preparedTransaction, new StorageCallback() {
                    @Override
                    public boolean onCompleted(long firstToken) {
                        latch.countDown();
                        return true;
                    }

                    @Override
                    public void onError(Throwable cause) {

                    }
                });
        });

        latch.await(5, TimeUnit.SECONDS);
        Thread.sleep(1500);
        testSubject.rollback(9899);
        Assert.assertEquals(9899, testSubject.getLastToken());

        testSubject.rollback(859);
        Assert.assertEquals(899, testSubject.getLastToken());
    }

    @Test
    public void rollbackAndRead() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(5);
        IntStream.range(0, 5).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, 3).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build());
            });
            PreparedTransaction preparedTransaction = testSubject.prepareTransaction(newEvents);
            testSubject.store(preparedTransaction, new StorageCallback() {
                @Override
                public boolean onCompleted(long firstToken) {
                    latch.countDown();
                    return true;
                }

                @Override
                public void onError(Throwable cause) {

                }
            });
        });

        latch.await(5, TimeUnit.SECONDS);
        Thread.sleep(1500);
        testSubject.rollback(2);
        Assert.assertEquals(2, testSubject.getLastToken());

        testSubject.initSegments(Long.MAX_VALUE);
        Assert.assertEquals(2, testSubject.getLastToken());
    }

}