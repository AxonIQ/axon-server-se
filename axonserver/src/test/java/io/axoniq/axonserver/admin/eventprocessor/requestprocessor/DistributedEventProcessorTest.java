/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */
package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class DistributedEventProcessorTest {

    @Test
    public void isStreamingWithoutStreamingFieldSet() {
        EventProcessorId id = new EventProcessorIdentifier("processor", "context", "tokenstore");
        ClientProcessor processor1 = clientProcessor("clientId", "Tracking", false);
        DistributedEventProcessor testSubject = new DistributedEventProcessor(id, Collections.singletonList(processor1));
        assertTrue(testSubject.isStreaming());
    }

    @Test
    public void isNotStreamingWithoutStreamingFieldSet() {
        EventProcessorId id = new EventProcessorIdentifier("processor", "context", "tokenstore");
        ClientProcessor processor1 = clientProcessor("clientId2", "Subscribing", false);
        DistributedEventProcessor testSubject = new DistributedEventProcessor(id, Collections.singletonList(processor1));
        assertFalse(testSubject.isStreaming());
    }

    @Test
    public void isStreaming() {
        EventProcessorId id = new EventProcessorIdentifier("processor", "context", "tokenstore");
        ClientProcessor processor1 = clientProcessor("clientId", "Tracking", true);
        DistributedEventProcessor testSubject = new DistributedEventProcessor(id, Collections.singletonList(processor1));
        assertTrue(testSubject.isStreaming());
    }

    @NotNull
    private ClientProcessor clientProcessor(String clientId, String mode, boolean streaming) {
        return new ClientProcessor() {
            @Override
            public String clientId() {
                return clientId;
            }

            @Override
            public String context() {
                return "context";
            }

            @Override
            public EventProcessorInfo eventProcessorInfo() {
                return EventProcessorInfo.newBuilder()
                                         .setMode(mode)
                                         .setIsStreamingProcessor(streaming)
                                         .build();
            }

            @Override
            public Boolean belongsToComponent(String component) {
                return true;
            }

            @Override
            public boolean belongsToContext(String context) {
                return true;
            }
        };
    }
}