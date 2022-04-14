/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.PlatformService.InstructionConsumer;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ProcessorEventPublisher}
 *
 * @author Sara Pellegrini
 */
public class ProcessorEventPublisherTest {

    private final String context = "context";
    private final String clientId = "clientId";
    private final String processor = "processor";
    private final String instructionId = "instructionId";

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void pauseProcessorRequest() {
        List<Object> events = new ArrayList<>();
        ProcessorEventPublisher testSubject = new ProcessorEventPublisher(i -> {}, events::add);
        testSubject.pauseProcessorRequest(context, clientId, processor, instructionId);
        assertEquals(1, events.size());
        PauseEventProcessorRequest event = (PauseEventProcessorRequest) events.get(0);
        assertEquals(context, event.context());
        assertEquals(clientId, event.clientId());
        assertEquals(processor, event.processorName());
        assertEquals(instructionId, event.instructionId());
    }

    @Test
    public void startProcessorRequest() {
        List<Object> events = new ArrayList<>();
        ProcessorEventPublisher testSubject = new ProcessorEventPublisher(i -> {}, events::add);
        testSubject.startProcessorRequest(context, clientId, processor, instructionId);
        assertEquals(1, events.size());
        StartEventProcessorRequest event = (StartEventProcessorRequest) events.get(0);
        assertEquals(context, event.context());
        assertEquals(clientId, event.clientId());
        assertEquals(processor, event.processorName());
        assertEquals(instructionId, event.instructionId());
    }

    @Test
    public void releaseSegment() {
        List<Object> events = new ArrayList<>();
        ProcessorEventPublisher testSubject = new ProcessorEventPublisher(i -> {}, events::add);
        testSubject.releaseSegment(context, clientId, processor, 3, instructionId);
        assertEquals(1, events.size());
        ReleaseSegmentRequest event = (ReleaseSegmentRequest) events.get(0);
        assertEquals(context, event.context());
        assertEquals(clientId, event.getClientId());
        assertEquals(processor, event.getProcessorName());
        assertEquals(instructionId, event.instructionId());
        assertEquals(3, event.getSegmentId());
    }

    @Test
    public void splitSegment() {
        List<Object> events = new ArrayList<>();
        ProcessorEventPublisher testSubject = new ProcessorEventPublisher(i -> {}, events::add);
        testSubject.splitSegment(context, clientId, processor, 3, instructionId);
        assertEquals(1, events.size());
        SplitSegmentRequest event = (SplitSegmentRequest) events.get(0);
        assertEquals(context, event.context());
        assertEquals(clientId, event.getClientId());
        assertEquals(processor, event.getProcessorName());
        assertEquals(instructionId, event.instructionId());
        assertEquals(3, event.getSegmentId());
    }

    @Test
    public void mergeSegment() {
        List<Object> events = new ArrayList<>();
        ProcessorEventPublisher testSubject = new ProcessorEventPublisher(i -> {}, events::add);
        testSubject.mergeSegment(context, clientId, processor, 3, instructionId);
        assertEquals(1, events.size());
        MergeSegmentRequest event = (MergeSegmentRequest) events.get(0);
        assertEquals(context, event.context());
        assertEquals(clientId, event.getClientId());
        assertEquals(processor, event.getProcessorName());
        assertEquals(instructionId, event.instructionId());
        assertEquals(3, event.getSegmentId());
    }

    @Test
    public void eventProcessorStatusReceived() {
        List<Object> events = new ArrayList<>();
        AtomicReference<InstructionConsumer> instructionConsumer = new AtomicReference<>();
        ProcessorEventPublisher testSubject = new ProcessorEventPublisher(instructionConsumer::set, events::add);
        testSubject.init();
        PlatformService.ClientComponent clientComponent = new PlatformService.ClientComponent(
                UUID.randomUUID() + clientId,
                clientId, "component", context);
        PlatformInboundInstruction instruction = PlatformInboundInstruction.newBuilder()
                                                                           .setEventProcessorInfo(EventProcessorInfo
                                                                                                          .newBuilder()
                                                                                                          .setProcessorName(
                                                                                                                  processor)
                                                                                                          .build())
                                                                           .build();
        instructionConsumer.get().accept(clientComponent, instruction);
        EventProcessorStatusUpdate update = (EventProcessorStatusUpdate) events.get(0);
        assertEquals(instruction.getEventProcessorInfo(), update.eventProcessorStatus().getEventProcessorInfo());
    }
}