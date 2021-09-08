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
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Controller;

/**
 * Validates event store transformation requests. There can only be one transformation per context. Each entry should
 * provide a valid previous token. When an event is replaced, the aggregate id and sequence number must remain the
 * same.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
@Controller
public class TransformationValidator {

    private final LocalEventStore localEventStore;
    private final TransformationCache transformationCache;

    public TransformationValidator(
            TransformationCache transformationCache,
            LocalEventStore localEventStore) {
        this.transformationCache = transformationCache;
        this.localEventStore = localEventStore;
    }

    public void validateDeleteEvent(String context, String transformationId, long token, long previousToken) {
        EventStoreTransformation transformation = transformationCache.get(transformationId);
        validateContext(context, transformation);
        validatePreviousToken(previousToken, transformation);
    }

    public void validateReplaceEvent(String context, String transformationId, long token, long previousToken,
                                     Event event) {
        EventStoreTransformation transformation = transformationCache.get(transformationId);
        validateContext(context, transformation);
        validatePreviousToken(previousToken, transformation);
        validateEventToReplace(token, event, context, transformation);
    }

    public void apply(String context, String transformationId, long lastEventToken) {
        EventStoreTransformation transformation = transformationCache.get(transformationId);
        validateContext(context, transformation);
        validatePreviousToken(lastEventToken, transformation);
    }

    public void cancel(String context, String transformationId) {
        EventStoreTransformation transformation = transformationCache.get(transformationId);
        validateContext(context, transformation);
        if (transformation.isApplying()) {
            throw new RuntimeException("Transformation in progress");
        }
    }

    private void validatePreviousToken(long previousToken, EventStoreTransformation transformation) {
        if (previousToken != transformation.getPreviousToken()) {
            throw new RuntimeException("Invalid previous token");
        }
    }

    private void validateContext(String context, EventStoreTransformation transformation) {
        if (transformation == null) {
            throw new RuntimeException("Transformation not found");
        }

        if (!transformation.getName().equals(context)) {
            throw new RuntimeException("Transformation id not valid for context");
        }
    }

    private void validateEventToReplace(long token, Event event, String context,
                                        EventStoreTransformation transformation) {
        if (event.getAggregateType().isEmpty()) {
            return;
        }

        CloseableIterator<SerializedEventWithToken> iterator = transformation.getIterator();
        if (iterator == null) {
            iterator = localEventStore.eventIterator(context, token);
            transformation.setIterator(iterator);
        }

        if (!iterator.hasNext()) {
            throw new RuntimeException("Event for token not found: " + token);
        }
        SerializedEventWithToken stored = iterator.next();
        while (stored.getToken() < token && iterator.hasNext()) {
            stored = iterator.next();
        }

        if (stored.getToken() != token) {
            throw new RuntimeException("Event for token not found: " + token);
        }

        if (!event.getAggregateIdentifier().equals(stored.getSerializedEvent().getAggregateIdentifier())) {
            throw new RuntimeException("Invalid aggregate identifier for: " + token);
        }
        if (event.getAggregateSequenceNumber() != stored.getSerializedEvent()
                                                        .getAggregateSequenceNumber()) {
            throw new RuntimeException("Invalid aggregate sequence number for: " + token);
        }
    }
}
