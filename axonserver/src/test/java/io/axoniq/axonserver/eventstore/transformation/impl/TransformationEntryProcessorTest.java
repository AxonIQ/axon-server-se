/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.axoniq.axonserver.localstorage.EventTransformationResult;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.springframework.data.util.CloseableIterator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since
 */
public class TransformationEntryProcessorTest {
    private final TransformationEntryProcessor testSubject = new TransformationEntryProcessor(this::iterator);
    private final List<TransformEventsRequest> requestList = new LinkedList<>();

    private CloseableIterator<TransformEventsRequest> iterator() {
        Iterator<TransformEventsRequest> wrapped = requestList.iterator();
        return new CloseableIterator<TransformEventsRequest>() {
            @Override
            public void close() {

            }

            @Override
            public boolean hasNext() {
                return wrapped.hasNext();
            }

            @Override
            public TransformEventsRequest next() {
                return wrapped.next();
            }
        };
    }

    @Test
    public void apply() {
        requestList.add(deleteEvent(100));
        requestList.add(deleteEvent(200));

        EventTransformationResult result = testSubject.apply(dummyEvent(), 100L);
        assertFalse(result.event().hasPayload());
        assertEquals(200L, result.nextToken());
    }

    @Test
    public void retryApply() {
        requestList.add(deleteEvent(100));
        requestList.add(updateEvent(200));

        EventTransformationResult result = testSubject.apply(dummyEvent(), 100L);
        assertFalse(result.event().hasPayload());
        assertEquals(200L, result.nextToken());

        result = testSubject.apply(dummyEvent(), 200L);
        assertTrue(result.event().hasPayload());
        assertEquals("UpdatedPayload", result.event().getPayload().getType());
        assertEquals(Long.MAX_VALUE, result.nextToken());

        result = testSubject.apply(dummyEvent(), 100L);
        assertFalse(result.event().hasPayload());
        assertEquals(200L, result.nextToken());
    }

    @Test
    public void applySingleDelete() {
        requestList.add(deleteEvent(100));

        EventTransformationResult result = testSubject.apply(dummyEvent(), 100L);
        assertFalse(result.event().hasPayload());
        assertEquals(Long.MAX_VALUE, result.nextToken());
    }

    @Test
    public void applyUnknown() {
        requestList.add(deleteEvent(100));

        Event event = dummyEvent();
        EventTransformationResult result = testSubject.apply(event, 1L);
        assertEquals(event, result.event());
        assertEquals(100, result.nextToken());
    }

    @Test
    public void applyEmpty() {
        Event event = dummyEvent();
        EventTransformationResult result = testSubject.apply(event, 1L);
        assertEquals(event, result.event());
        assertEquals(Long.MAX_VALUE, result.nextToken());
    }

    private Event dummyEvent() {
        return Event.newBuilder()
                .setPayload(SerializedObject.newBuilder()
                                    .setType("DummyPayloadType")
                                            .build())
                    .build();
    }

    @NotNull
    private TransformEventsRequest deleteEvent(long token) {
        return TransformEventsRequest.newBuilder()
                                .setDeleteEvent(DeletedEvent.newBuilder()
                                                        .setToken(token))
                                              .build();
    }
    @NotNull
    private TransformEventsRequest updateEvent(long token) {
        return TransformEventsRequest.newBuilder()
                                     .setEvent(TransformedEvent.newBuilder()
                                                       .setEvent(Event.newBuilder()
                                                                      .setPayload(SerializedObject.newBuilder()
                                                                                                  .setType("UpdatedPayload")
                                                                                                  .build())
                                                                      .build())
                                                               .setToken(token))
                                     .build();
    }
}