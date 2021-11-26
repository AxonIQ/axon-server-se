/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationStateManager;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationProcessor;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationValidator;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.UUID;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class DefaultEventStoreTransformationService implements EventStoreTransformationService {

    private final TransformationStateManager transformationStateManager;
    private final TransformationValidator transformationValidator;
    private final TransformationProcessor transformationProcessor;
    private final LocalEventStore localEventStore;

    public DefaultEventStoreTransformationService(
            TransformationStateManager transformationStateManager,
            TransformationValidator transformationValidator,
            TransformationProcessor transformationProcessor,
            LocalEventStore localEventStore) {
        this.transformationStateManager = transformationStateManager;
        this.transformationValidator = transformationValidator;
        this.transformationProcessor = transformationProcessor;
        this.localEventStore = localEventStore;
    }

    @Override
    public Mono<String> startTransformation(String context, String description) {
        return Mono.create(sink -> {
            String id = UUID.randomUUID().toString();
            transformationStateManager.reserve(context, id);
            try {
                transformationProcessor.startTransformation(context, id,
                                                            transformationStateManager.nextVersion(context),
                                                            description);
            } catch (RuntimeException runtimeException) {
                transformationStateManager.delete(id);
                throw runtimeException;
            }
            sink.success(id);
        });
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token, long previousToken) {
        return Mono.defer(() -> {
            transformationValidator.validateDeleteEvent(context, transformationId, token, previousToken);
            return transformationProcessor.deleteEvent(transformationId, token);
        });
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event,
                                   long previousToken) {
        return Mono.defer(() -> {
            transformationValidator.validateReplaceEvent(context, transformationId, token, previousToken, event);
            return transformationProcessor.replaceEvent(transformationId, token, event);
        });
    }

    @Override
    public Mono<Void> cancelTransformation(String context, String id) {
        return Mono.create(sink -> {
            transformationValidator.cancel(context, id);
            transformationProcessor.cancel(id);
            sink.success();
        });
    }

    @Override
    public Mono<Void> applyTransformation(String context, String transformationId, long lastEventToken,
                                          boolean keepOldVersions,
                                          String appliedBy) {
        return Mono.fromCompletionStage(()  -> {
            transformationValidator.apply(context, transformationId, lastEventToken);
            return transformationProcessor.apply(transformationId, localEventStore.keepOldVersions(context) || keepOldVersions,
                                                 appliedBy,
                                                 new Date(), transformationStateManager.firstToken(transformationId), lastEventToken)
                                   .thenAccept(result -> transformationProcessor.complete(transformationId));

        });
    }

    @Override
    public Mono<Void> rollbackTransformation(String context, String id) {
        return Mono.create(sink -> {
            transformationValidator.rollback(context, id);
            transformationProcessor.rollbackTransformation(context, id);
            sink.success();
        });
    }

    @Override
    public Mono<Void> deleteOldVersions(String context, String id) {
        return Mono.create(sink -> {
            transformationValidator.deleteOldVersions(context, id);
            transformationProcessor.deleteOldVersions(context, id);
            sink.success();
        });
    }
}
