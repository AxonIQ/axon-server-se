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
import io.axoniq.axonserver.grpc.event.PayloadDescription;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.springframework.data.util.CloseableIterator;

import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class TrackingEventProcessorManagerTest {

    private TrackingEventProcessorManager testSubject;
    private AtomicInteger eventsLeft = new AtomicInteger(10);
    private AtomicBoolean iteratorClosed = new AtomicBoolean();

    @Before
    public void setup() {
        Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorBuilder = (token) -> {
            AtomicLong nextToken = new AtomicLong(token);

            return new CloseableIterator<SerializedEventWithToken>() {
                @Override
                public void close() {
                    iteratorClosed.set(true);
                }

                @Override
                public boolean hasNext() {
                    return eventsLeft.get() > 0;
                }

                @Override
                public SerializedEventWithToken next() {
                    eventsLeft.decrementAndGet();
                    return new SerializedEventWithToken(nextToken.getAndIncrement(),
                                                        Event.newBuilder()
                                                             .setPayload(SerializedObject.newBuilder()
                                                             .setType("DemoType")
                                                             .setRevision("1.0"))
                                                             .build());
                }
            };
        };
        testSubject = new TrackingEventProcessorManager("demo", iteratorBuilder, 5);
    }

    @Test
    public void createEventTracker() throws InterruptedException {
        AtomicInteger messagesReceived = new AtomicInteger();
        AtomicBoolean completed = new AtomicBoolean();
        AtomicBoolean failed = new AtomicBoolean();
        TrackingEventProcessorManager.EventTracker tracker =
                testSubject
                        .createEventTracker(100L,
                                            "",
                                            true,
                                            new StreamObserver<InputStream>() {
                                                @Override
                                                public void onNext(InputStream value) {
                                                    messagesReceived.incrementAndGet();
                                                }

                                                @Override
                                                public void onError(Throwable t) {
                                                       failed.set(true);
                                                   }

                                                   @Override
                                                   public void onCompleted() {
                                                       completed.set(true);
                                                   }
                                               });
        tracker.addPermits(5);
        tracker.start();

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(5, messagesReceived.get()));
        tracker.addPermits(10);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(10, messagesReceived.get()));

        eventsLeft.set(10);
        testSubject.reschedule();
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(15, messagesReceived.get()));

        tracker.stop();
        assertTrue(completed.get());
        assertWithin(10, TimeUnit.MILLISECONDS, () -> assertTrue(iteratorClosed.get()));
    }


    @Test
    public void blacklist() throws InterruptedException {
        eventsLeft.set(50);
        AtomicInteger messagesReceived = new AtomicInteger();
        AtomicBoolean completed = new AtomicBoolean();
        AtomicBoolean failed = new AtomicBoolean();
        TrackingEventProcessorManager.EventTracker tracker =
                testSubject.createEventTracker(100L,
                                               "",
                                               true,
                                               new StreamObserver<InputStream>() {
                                                   @Override
                                                   public void onNext(
                                                           InputStream value) {
                                                       messagesReceived.incrementAndGet();
                                                   }

                                                   @Override
                                                   public void onError(Throwable t) {
                                                       failed.set(true);
                                                   }

                                                   @Override
                                                   public void onCompleted() {
                                                       completed.set(true);
                                                   }
                                               });
        tracker.addPermits(50);
        tracker.addBlacklist(Collections.singletonList(PayloadDescription.newBuilder()
                                                                         .setType("DemoType")
                                                                         .setRevision("1.0")
                                                                         .build()));
        tracker.start();
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(10, messagesReceived.get()));
    }

    @Test
    public void testStopAllWhereRequestIsNotForLocalStoreOnly() throws InterruptedException {
        AtomicInteger useLocalStoreMessagesReceived = new AtomicInteger();
        AtomicBoolean useLocalStoreCompleted = new AtomicBoolean();
        AtomicBoolean useLocalStoreFailed = new AtomicBoolean();

        TrackingEventProcessorManager.EventTracker useLocalStoreTracker = testSubject.createEventTracker(
                100,
                "",
                false,
                new StreamObserver<InputStream>() {
                    @Override
                    public void onNext(InputStream value) {
                        useLocalStoreMessagesReceived.incrementAndGet();
                    }

                    @Override
                    public void onError(Throwable t) {
                        useLocalStoreFailed.set(true);
                    }

                    @Override
                    public void onCompleted() {
                        useLocalStoreCompleted.set(true);
                    }
                });
        useLocalStoreTracker.addPermits(5);
        useLocalStoreTracker.start();

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(5, useLocalStoreMessagesReceived.get()));
        assertFalse(useLocalStoreCompleted.get());
        assertFalse(useLocalStoreFailed.get());

        testSubject.stopAllWhereNotAllowedReadingFromFollower();

        useLocalStoreTracker.addPermits(5);

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(10, useLocalStoreMessagesReceived.get()));
    }
}