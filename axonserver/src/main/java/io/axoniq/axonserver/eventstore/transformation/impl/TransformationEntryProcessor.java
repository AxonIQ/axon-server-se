/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformEventRequest;
import io.axoniq.axonserver.localstorage.EventTransformationResult;
import org.springframework.data.util.CloseableIterator;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class TransformationEntryProcessor implements BiFunction<Event, Long, EventTransformationResult> {
    private final AtomicLong lastToken = new AtomicLong(-1);
    private final AtomicReference<CloseableIterator<TransformEventRequest>> iteratorHolder = new AtomicReference<>();
    private final AtomicReference<TransformEventRequest> nextEntry = new AtomicReference<>();
    private final Supplier<CloseableIterator<TransformEventRequest>> iteratorFactory;

    public TransformationEntryProcessor(Supplier<CloseableIterator<TransformEventRequest>> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    @Override
    public EventTransformationResult apply(Event event, Long token) {
        if (token < lastToken.get()) {
            close();
            nextEntry.set(null);
        }


        initTransformationAtToken(token);
        lastToken.set(token);

        if (nextEntry.get() == null) {
            return eventTransformationResult(event, Long.MAX_VALUE);
        }

        TransformEventRequest request = nextEntry.get();
        if (token(request) == token) {
            event = applyTransformation(event, request);
            if (!iteratorHolder.get().hasNext()) {
                return eventTransformationResult(event, Long.MAX_VALUE);
            }

            nextEntry.set(iteratorHolder.get().next());
        }

        return eventTransformationResult(event, token(nextEntry.get()));
    }

    private Event applyTransformation(Event original, TransformEventRequest transformRequest) {
        switch (transformRequest.getRequestCase()) {
            case EVENT:
                return merge(original, transformRequest.getEvent().getEvent());
            case DELETE_EVENT:
                return nullify(original);
            case REQUEST_NOT_SET:
                break;
        }
        return original;
    }

    private Event merge(Event original, Event updated) {
        return Event.newBuilder(original)
                    .clearMetaData()
                    .setAggregateType(updated.getAggregateType())
                    .setPayload(updated.getPayload())
                    .putAllMetaData(updated.getMetaDataMap())
                    .build();
    }

    private Event nullify(Event event) {
        return Event.newBuilder(event)
                    .clearPayload()
                    .clearMessageIdentifier()
                    .clearMetaData()
                    .build();
    }

    private EventTransformationResult eventTransformationResult(Event event, long nextToken) {
        return new EventTransformationResult(){

            @Override
            public Event event() {
                return event;
            }

            @Override
            public long nextToken() {
                return nextToken;
            }
        };
    }

    private long token(TransformEventRequest request) {
        switch (request.getRequestCase()) {
            case EVENT:
                return request.getEvent().getToken();
            case DELETE_EVENT:
                return request.getDeleteEvent().getToken();
            default:
                throw new IllegalArgumentException("Request without token");
        }
    }

    private void initTransformationAtToken(Long token) {
        if (nextEntry.get() == null) {
            CloseableIterator<TransformEventRequest> iterator = iteratorFactory.get();
            boolean found = false;
            while (!found && iterator.hasNext()) {
                TransformEventRequest request = iterator.next();
                if (token(request) >= token) {
                    found = true;
                    nextEntry.set(request);
                }
            }
            iteratorHolder.set(iterator);
        }
    }

    public void close() {
        CloseableIterator<TransformEventRequest> currentIterator = iteratorHolder.getAndSet(null);
        if (currentIterator != null) {
            currentIterator.close();
        }
    }
}
