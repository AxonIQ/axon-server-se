/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.CancelScheduledEventRequest;
import io.axoniq.axonserver.grpc.event.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleToken;
import io.axoniq.axonserver.taskscheduler.JacksonTaskPayloadSerializer;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class EventSchedulerServiceTest {

    private StandaloneTaskManager standaloneTaskManager = mock(StandaloneTaskManager.class);
    private EventSchedulerService testSubject;

    @Before
    public void setup() {
        when(standaloneTaskManager.createTask(anyString(), any(), anyLong())).thenReturn("1234");
        testSubject = new EventSchedulerService(standaloneTaskManager, new JacksonTaskPayloadSerializer());
    }

    @Test
    public void scheduleEvent() {
        ScheduleEventRequest request = ScheduleEventRequest.newBuilder()
                                                           .build();
        FakeStreamObserver<ScheduleToken> responseObserver = new FakeStreamObserver<>();
        testSubject.scheduleEvent(request, responseObserver);
        assertFalse(responseObserver.values().isEmpty());
        ScheduleToken scheduleToken = responseObserver.values().get(0);
        assertEquals("1234", scheduleToken.getToken());
        assertEquals(1, responseObserver.completedCount());
    }

    @Test
    public void rescheduleEventWithoutPreviousToken() {
        FakeStreamObserver<ScheduleToken> responseObserver = new FakeStreamObserver<>();
        RescheduleEventRequest request = RescheduleEventRequest.getDefaultInstance();
        testSubject.rescheduleEvent(request, responseObserver);
        assertFalse(responseObserver.values().isEmpty());
        ScheduleToken scheduleToken = responseObserver.values().get(0);
        assertEquals("1234", scheduleToken.getToken());
        verify(standaloneTaskManager, never()).cancel(anyString());
        assertEquals(1, responseObserver.completedCount());
    }

    @Test
    public void rescheduleEventWithPreviousToken() {
        FakeStreamObserver<ScheduleToken> responseObserver = new FakeStreamObserver<>();
        RescheduleEventRequest request = RescheduleEventRequest.newBuilder()
                                                               .setToken("123")
                                                               .build();
        testSubject.rescheduleEvent(request, responseObserver);
        assertFalse(responseObserver.values().isEmpty());
        ScheduleToken scheduleToken = responseObserver.values().get(0);
        assertEquals("1234", scheduleToken.getToken());
        verify(standaloneTaskManager).cancel("123");
        assertEquals(1, responseObserver.completedCount());
    }

    @Test
    public void cancelScheduledEvent() {
        FakeStreamObserver<InstructionAck> responseObserver = new FakeStreamObserver<>();
        CancelScheduledEventRequest request = CancelScheduledEventRequest.newBuilder()
                                                                         .setToken("123")
                                                                         .build();
        testSubject.cancelScheduledEvent(request, responseObserver);
        assertFalse(responseObserver.values().isEmpty());
        InstructionAck ack = responseObserver.values().get(0);
        assertTrue(ack.getSuccess());
        verify(standaloneTaskManager).cancel("123");
        assertEquals(1, responseObserver.completedCount());
    }
}