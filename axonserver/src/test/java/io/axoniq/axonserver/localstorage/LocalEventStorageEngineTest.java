/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static io.axoniq.axonserver.util.AssertUtils.assertWithin;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class LocalEventStorageEngineTest {
    private LocalEventStore testSubject;
    private static final String SAMPLE_CONTEXT = "FakeContext";
    private List<CompletableFuture<Long>> pendingTransactions = new ArrayList<>();

    @Before
    public void setup() {
        testSubject = new LocalEventStore(new EventStoreFactory() {
            @Override
            public EventStorageEngine createEventStorageEngine(String context) {
                return new FakeEventStore(EventType.EVENT);
            }

            @Override
            public EventStorageEngine createSnapshotStorageEngine(String context) {
                return new FakeEventStore(EventType.SNAPSHOT);
            }

            @Override
            public StorageTransactionManager createTransactionManager(EventStorageEngine eventStorageEngine) {
                return new StorageTransactionManager() {
                    @Override
                    public CompletableFuture<Long> store(List<SerializedEvent> eventList) {
                        CompletableFuture<Long> pendingTransaction = new CompletableFuture<>();
                        pendingTransactions.add(pendingTransaction);
                        return pendingTransaction;
                    }

                    @Override
                    public Runnable reserveSequenceNumbers(List<SerializedEvent> eventList) {
                        return () -> {
                        };
                    }

                    @Override
                    public void cancelPendingTransactions() {
                        pendingTransactions.forEach(p -> p.completeExceptionally(new RuntimeException("Transaction cancelled")));
                    }

                    @Override
                    public void deleteAllEventData() {

                    }
                };
            }
        }, 5, 1000);
        testSubject.initContext(SAMPLE_CONTEXT, false);
    }

    @Test
    public void cleanupContext() {
        testSubject.cleanupContext(SAMPLE_CONTEXT, false);
    }

    @Test
    public void cancel() {
        CountingStreamObserver<Confirmation> countingStreamObserver = new CountingStreamObserver<>();
        StreamObserver<InputStream> connection = testSubject.createAppendEventConnection(SAMPLE_CONTEXT, countingStreamObserver);
        connection.onNext(new ByteArrayInputStream(Event.newBuilder().build().toByteArray()));
        connection.onCompleted();

        testSubject.cancel(SAMPLE_CONTEXT);
        assertEquals(1, pendingTransactions.size());
        assertNotNull(countingStreamObserver.error);
        assertTrue(pendingTransactions.get(0).isDone());
    }

    @Test
    public void appendSnapshot() {
        CompletableFuture<Confirmation> snapshot = testSubject.appendSnapshot(SAMPLE_CONTEXT,
                                                                              Event.newBuilder()
                                                                                   .setAggregateIdentifier("123")
                                                                                   .setAggregateSequenceNumber(100).build());
        try {
            assertEquals(1, pendingTransactions.size());
            pendingTransactions.get(0).complete(100L);
            snapshot.get(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createAppendEventConnection() {
        CountingStreamObserver<Confirmation> countingStreamObserver = new CountingStreamObserver<>();
        StreamObserver<InputStream> connection = testSubject.createAppendEventConnection(SAMPLE_CONTEXT, countingStreamObserver);
        connection.onNext(new ByteArrayInputStream(Event.newBuilder().build().toByteArray()));
        connection.onCompleted();

        assertEquals(1, pendingTransactions.size());
        pendingTransactions.get(0).complete(100L);
        assertNull(countingStreamObserver.error);
        assertEquals(1, countingStreamObserver.responseList.size());
        assertTrue(pendingTransactions.get(0).isDone());
    }

    @Test
    public void createAppendEventConnectionWithTooManyEvents() {
        CountingStreamObserver<Confirmation> countingStreamObserver = new CountingStreamObserver<>();
        StreamObserver<InputStream> connection = testSubject.createAppendEventConnection(SAMPLE_CONTEXT, countingStreamObserver);
        IntStream.range(0, 10).forEach(i -> connection.onNext(new ByteArrayInputStream(Event.newBuilder().build().toByteArray())));
        connection.onCompleted();

        assertEquals(0, pendingTransactions.size());
        assertNotNull(countingStreamObserver.error);
    }

    @Test
    public void listAggregateEvents() {
    }

    @Test
    public void listAggregateSnapshots() {
    }

    @Test
    public void listEvents() throws InterruptedException {
        CountingStreamObserver<InputStream> countingStreamObserver = new CountingStreamObserver<>();
        StreamObserver<GetEventsRequest> requestStreamObserver = testSubject.listEvents(
                SAMPLE_CONTEXT,
                countingStreamObserver);
        requestStreamObserver.onNext(GetEventsRequest.newBuilder()
                                                     .setTrackingToken(100)
                                                     .setNumberOfPermits(10)
                                                     .build());
        assertWithin(2000, TimeUnit.MILLISECONDS, () -> assertEquals(10, countingStreamObserver.count));
        requestStreamObserver.onNext(GetEventsRequest.newBuilder()
                                                     .setNumberOfPermits(10)
                                                     .build());
        assertWithin(2000, TimeUnit.MILLISECONDS, () -> assertEquals(20, countingStreamObserver.count));

        requestStreamObserver.onCompleted();
    }

    @Test
    public void getFirstToken() {
    }

    @Test
    public void getLastToken() {
    }

    @Test
    public void getTokenAt() {
    }

    @Test
    public void readHighestSequenceNr() {
    }

    @Test
    public void queryEvents() {
    }

    @Test
    public void isAutoStartup() {
    }

    @Test
    public void checkHeartbeat() {

    }

    @Test
    public void checkPermits() {
    }

    @Test
    public void getLastToken1() {
    }

    @Test
    public void getLastSnapshot() {
    }

    @Test
    public void streamEventTransactions() {
    }

    @Test
    public void streamSnapshotTransactions() {
    }

    @Test
    public void syncEvents() {
    }

    @Test
    public void syncSnapshots() {
    }

    @Test
    public void getWaitingEventTransactions() {
    }

    @Test
    public void getWaitingSnapshotTransactions() {
    }

    @Test
    public void getLastCommittedToken() {
    }

    @Test
    public void getLastCommittedSnapshot() {
    }

    @Test
    public void rollbackEvents() {
    }

    @Test
    public void rollbackSnapshots() {
    }

    @Test
    public void getBackupFilenames() {
    }

    @Test
    public void health() {
    }

    @Test
    public void containsEvents() {
    }

    @Test
    public void containsSnapshots() {
    }

    @Test
    public void getLastCommitted() {
    }
}