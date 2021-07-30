/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import org.junit.jupiter.api.*;
import reactor.test.StepVerifier;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Marc Gathier
 * @since 4.5.6
 */
class EventSourceFluxTest {

    public static final int READ_ITERATIONS = 10;

    @Test
    void get() throws InterruptedException {

        AtomicInteger closedCount = new AtomicInteger();

        EventSourceFactory eventSourceFactory = new EventSourceFactory() {

            @Override
            public Optional<EventSource> create() {
                return Optional.of(new EventSource() {

                    final AtomicBoolean closed = new AtomicBoolean();

                    @Override
                    public SerializedEvent readEvent(int position) {
                        if (closed.get()) {
                            throw new IllegalStateException("Attempting to read from closed stream");
                        }
                        return new SerializedEvent(Event.getDefaultInstance());
                    }

                    @Override
                    public TransactionIterator createTransactionIterator(long segment, long token, boolean validating) {
                        return null;
                    }

                    @Override
                    public EventIterator createEventIterator(long segment, long startToken) {
                        return null;
                    }

                    @Override
                    public void close() {
                        closed.set(true);
                        closedCount.incrementAndGet();
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        };

        for (int i = 0; i < READ_ITERATIONS; i++) {
            EventSourceFlux eventSourceFlux = new EventSourceFlux(new StandardIndexEntries(0, createIntArray(100)),
                                                                  eventSourceFactory,
                                                                  100, 5);

            StepVerifier.create(eventSourceFlux.get())
                        .expectNextCount(100)
                        .verifyComplete();
        }

        // the close is called asynchronously, so we will
        assertWithin(100, TimeUnit.MILLISECONDS, () -> assertEquals(READ_ITERATIONS, closedCount.get()));
    }

    private Integer[] createIntArray(int i) {
        Integer[] ints = new Integer[i];
        for (int j = 0; j < i; j++) {
            ints[j] = i;
        }
        return ints;
    }
}