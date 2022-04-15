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
import io.axoniq.axonserver.localstorage.LocalEventStoreTransformer;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Controller;

import static java.lang.String.format;

/**
 * Validates event store transformation requests. There can only be one transformation per context. Each entry should
 * provide a valid previous token. When an event is replaced, the aggregate id and sequence number must remain the
 * same.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
@Controller
public class DefaultTransformationValidator implements TransformationValidator {

    private final LocalEventStore localEventStore;
    private final TransformationStateManager transformationStateManager;

    public DefaultTransformationValidator(
            TransformationStateManager transformationStateManager,
            LocalEventStore localEventStore) {
        this.transformationStateManager = transformationStateManager;
        this.localEventStore = localEventStore;
    }

    @Override
    public void validateDeleteEvent(String context, String transformationId, long token, long previousToken) {
        ActiveEventStoreTransformation transformation = transformationStateManager.get(transformationId);
        validateContext(context, transformation);
        validateTokenIncreasing(token, previousToken);
        validatePreviousToken(previousToken, transformation);
    }

    private void validateTokenIncreasing(long token, long previousToken) {
        if (token <= previousToken) {
            throw new RuntimeException("Invalid sequence of events in transformation");
        }
    }

    @Override
    public void validateReplaceEvent(String context, String transformationId, long token, long previousToken,
                                     Event event) {
        ActiveEventStoreTransformation transformation = transformationStateManager.get(transformationId);
        validateContext(context, transformation);
        validateTokenIncreasing(token, previousToken);
        validatePreviousToken(previousToken, transformation);
        validateEventToReplace(token, event, context, transformation);
    }

    @Override
    public void validateApply(String context, String transformationId, long lastEventToken) {
        ActiveEventStoreTransformation transformation = transformationStateManager.get(transformationId);
        validateContext(context, transformation);
        validatePreviousToken(lastEventToken, transformation);
    }

    @Override
    public void validateCancel(String context, String transformationId) {
        ActiveEventStoreTransformation transformation = transformationStateManager.get(transformationId);
        validateContext(context, transformation);
        if (transformation.applying()) {
            throw new RuntimeException("Transformation in progress");
        }
    }

    private void validatePreviousToken(long previousToken, ActiveEventStoreTransformation transformation) {
        if (previousToken != transformation.lastToken()) {
            throw new RuntimeException(format("Invalid previous token %d, expecting %d",
                                              previousToken,
                                              transformation.lastToken()));
        }
    }

    private void validateContext(String context, ActiveEventStoreTransformation transformation) {
        if (transformation == null) {
            throw new RuntimeException("Transformation not found");
        }

        if (!transformation.context().equals(context)) {
            throw new RuntimeException("Transformation id not valid for context");
        }
    }

    private void validateEventToReplace(long token, Event event, String context,
                                        ActiveEventStoreTransformation transformation) {
        if (event.getAggregateType().isEmpty()) {
            return;
        }

        CloseableIterator<SerializedEventWithToken> iterator = transformation.iterator();
        if (iterator == null) {
            iterator = localEventStore.eventIterator(context, token);
            transformationStateManager.setIteratorForActiveTransformation(transformation.id(), iterator);
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
            transformationStateManager.setIteratorForActiveTransformation(transformation.id(), null);
            throw new RuntimeException("Invalid aggregate identifier for: " + token);
        }
        if (event.getAggregateSequenceNumber() != stored.getSerializedEvent()
                                                        .getAggregateSequenceNumber()) {
            transformationStateManager.setIteratorForActiveTransformation(transformation.id(), null);
            throw new RuntimeException("Invalid aggregate sequence number for: " + token);
        }
    }

    @Override
    public void validateDeleteOldVersions(String context, String transformationId) {
        EventStoreTransformationJpa transformation = transformationStateManager.transformation(transformationId)
                                                                               .orElseThrow(() -> new RuntimeException(
                                                                                       "Transformation not found"));
        transformationKeepingOldVersions(context, transformation);
    }

    private void transformationKeepingOldVersions(String context, EventStoreTransformationJpa transformation) {
        if (!transformation.getContext().equals(context)) {
            throw new RuntimeException("Transformation id not valid for context");
        }

        if (!EventStoreTransformationJpa.Status.DONE.equals(transformation.getStatus())) {
            throw new RuntimeException("Transformation is not completed yet");
        }

        if (!transformation.isKeepingOldVersions()) {
            throw new RuntimeException("Transformation started without keep old versions option");
        }
    }

    @Override
    public void validateRollback(String context, String transformationId) {
        EventStoreTransformationJpa transformation = transformationStateManager.transformation(transformationId)
                                                                               .orElseThrow(() -> new RuntimeException(
                                                                                       "Transformation not found"));
        transformationKeepingOldVersions(context, transformation);
        LocalEventStoreTransformer.Result canRollback = localEventStore.canRollbackTransformation(
                context,
                transformation.getVersion(),
                transformation.getFirstEventToken(),
                transformation.getLastEventToken());
        if (!canRollback.accepted()) {
            throw new RuntimeException(canRollback.reason());
        }
    }
}
