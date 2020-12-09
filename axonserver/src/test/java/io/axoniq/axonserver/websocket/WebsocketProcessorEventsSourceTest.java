/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.websocket;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import org.junit.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class WebsocketProcessorEventsSourceTest {

    @Test
    public void on() throws InterruptedException {
        AtomicInteger triggers = new AtomicInteger();
        WebsocketProcessorEventsSource testSubject = new WebsocketProcessorEventsSource(() -> {
            triggers.incrementAndGet();
        });
        ScheduledFuture<?> scheduler = Executors.newSingleThreadScheduledExecutor()
                                                .scheduleAtFixedRate(testSubject::applyIfUpdates,
                                                                     10,
                                                                     10,
                                                                     TimeUnit.MILLISECONDS);

        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null));
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null));
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null));
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null));
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null));
        Thread.sleep(50);
        assertEquals(1, triggers.get());
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null));
        Thread.sleep(50);
        assertEquals(2, triggers.get());

        scheduler.cancel(false);
    }

    @Test
    public void onException() throws InterruptedException {
        AtomicInteger triggers = new AtomicInteger();
        WebsocketProcessorEventsSource testSubject = new WebsocketProcessorEventsSource(() -> {
            if (triggers.getAndIncrement() == 5) {
                throw new IllegalArgumentException("Failed");
            }
        });
        ScheduledFuture<?> scheduler = Executors.newSingleThreadScheduledExecutor()
                                                .scheduleAtFixedRate(testSubject::applyIfUpdates,
                                                                     50,
                                                                     50,
                                                                     TimeUnit.MILLISECONDS);

        IntStream.range(0, 50).parallel().forEach(i -> testSubject
                .on(new EventProcessorEvents.EventProcessorStatusUpdate(null)));
        Thread.sleep(100);

        assertEquals(1, triggers.get());
        scheduler.cancel(false);
    }
}