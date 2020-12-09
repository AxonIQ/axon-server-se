/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.listener;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.test.FakeClock;
import org.junit.*;

import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ProcessorsInfoTargetTest {

    private final FakeClock clock = new FakeClock();
    private final ProcessorsInfoTarget testSubject = new ProcessorsInfoTarget(clock, 100);

    @Test
    public void onEventProcessorStatusChange() {
        EventProcessorInfo processorInfo = EventProcessorInfo.newBuilder()
                                                             .setActiveThreads(10)
                                                             .setAvailableThreads(20)
                                                             .setProcessorName("Name")
                                                             .build();
        ClientEventProcessorInfo clientEventProcessorInfo = new ClientEventProcessorInfo("client", "client",
                                                                                         "context",
                                                                                         processorInfo);
        EventProcessorStatusUpdate event = new EventProcessorStatusUpdate(clientEventProcessorInfo);
        EventProcessorEvents.EventProcessorStatusUpdated updatedEvent = testSubject
                .onEventProcessorStatusChange(event);
        assertEquals("client", updatedEvent.eventProcessorStatus().getClientId());
        assertEquals("context", updatedEvent.eventProcessorStatus().getContext());
        ClientProcessor clientProcessor = StreamSupport.stream(Spliterators.spliterator(testSubject.iterator(), 100, 0),
                                                               false).
                                                               filter(cp -> cp.clientId().equals("client"))
                                                       .findFirst().orElse(null);
        assertNotNull(clientProcessor);
        clock.timeElapses(120);
        clientProcessor = StreamSupport.stream(Spliterators.spliterator(testSubject.iterator(), 100, 0),
                                               false).
                                               filter(cp -> cp.clientId().equals("client"))
                                       .findFirst().orElse(null);
        assertNull(clientProcessor);
    }

}