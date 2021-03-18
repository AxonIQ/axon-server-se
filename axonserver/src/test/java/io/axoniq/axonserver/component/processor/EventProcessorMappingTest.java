/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;

import java.util.Collections;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Test class validating the {@link EventProcessorMapping}.
 *
 * @author Sara Pellegrini
 */
public class EventProcessorMappingTest {

    private static final String PROCESSOR_NAME = "name";
    private static final String CLIENT_ID = "id";
    private static final String SUBSCRIBING_MODE = "Subscribing";
    private static final String TRACKING_MODE = "Tracking";
    private static final String POOLED_MODE = "Pooled Streaming";

    private final EventProcessorMapping testSubject = new EventProcessorMapping();

    @Test
    public void testTrackingMode() {
        EventProcessorInfo testProcessorInfo = EventProcessorInfo.newBuilder()
                                                                 .setMode(TRACKING_MODE)
                                                                 .setIsStreamingProcessor(true)
                                                                 .build();
        ClientProcessor testProcessor = new FakeClientProcessor(CLIENT_ID, true, testProcessorInfo);

        EventProcessor result = testSubject.apply(PROCESSOR_NAME, asList(testProcessor, testProcessor));

        assertTrue(result instanceof StreamingProcessor);
        assertEquals(TRACKING_MODE, result.mode());
        assertTrue(result.isStreaming());
    }

    @Test
    public void testGeneric() {
        ClientProcessor clientProcessor = new FakeClientProcessor(
                CLIENT_ID, true, EventProcessorInfo.newBuilder().setMode(SUBSCRIBING_MODE).build()
        );

        EventProcessor processor = testSubject.apply(PROCESSOR_NAME, asList(clientProcessor, clientProcessor));

        assertTrue(processor instanceof GenericProcessor);
        assertEquals(SUBSCRIBING_MODE, processor.mode());
        assertFalse(processor.isStreaming());
    }


    @Test
    public void testPooledMode() {
        EventProcessorInfo testProcessorInfo = EventProcessorInfo.newBuilder()
                                                                 .setMode(POOLED_MODE)
                                                                 .setIsStreamingProcessor(true)
                                                                 .build();
        ClientProcessor testProcessor = new FakeClientProcessor(CLIENT_ID, true, testProcessorInfo);

        EventProcessor result = testSubject.apply(PROCESSOR_NAME, Collections.singletonList(testProcessor));

        assertTrue(result instanceof StreamingProcessor);
        assertEquals(POOLED_MODE, result.mode());
    }

    @Test
    public void testMixed() {
        ClientProcessor tracking = new FakeClientProcessor(
                CLIENT_ID, true, EventProcessorInfo.newBuilder().setMode(TRACKING_MODE).build()
        );

        ClientProcessor subscribing = new FakeClientProcessor(
                CLIENT_ID, true, EventProcessorInfo.newBuilder().setMode(SUBSCRIBING_MODE).build()
        );

        EventProcessor processor = testSubject.apply(PROCESSOR_NAME, asList(tracking, subscribing));

        assertTrue(processor instanceof GenericProcessor);
        assertNotEquals(TRACKING_MODE, processor.mode());
        assertNotEquals(SUBSCRIBING_MODE, processor.mode());
    }
}