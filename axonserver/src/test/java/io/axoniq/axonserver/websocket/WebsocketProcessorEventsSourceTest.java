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

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class WebsocketProcessorEventsSourceTest {

    @Test
    public void on() throws InterruptedException {
        AtomicInteger triggers = new AtomicInteger();
        WebsocketProcessorEventsSource testSubject = new WebsocketProcessorEventsSource(e -> {
            triggers.incrementAndGet();
        }, 10);

        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null, true));
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null, true));
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null, true));
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null, true));
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null, true));
        Thread.sleep(20);
        assertEquals(1, triggers.get());
        testSubject.on(new EventProcessorEvents.EventProcessorStatusUpdate(null, true));
        Thread.sleep(15);
        assertEquals(2, triggers.get());
    }
}