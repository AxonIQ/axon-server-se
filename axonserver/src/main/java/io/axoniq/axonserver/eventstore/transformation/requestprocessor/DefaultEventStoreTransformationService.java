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
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationCache;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationProcessor;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationValidator;
import io.axoniq.axonserver.grpc.event.Event;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class DefaultEventStoreTransformationService implements EventStoreTransformationService {

    private final TransformationCache transformationCache;
    private final TransformationValidator transformationValidator;
    private final TransformationProcessor transformationProcessor;

    public DefaultEventStoreTransformationService(
            TransformationCache transformationCache,
            TransformationValidator transformationValidator,
            TransformationProcessor transformationProcessor) {
        this.transformationCache = transformationCache;
        this.transformationValidator = transformationValidator;
        this.transformationProcessor = transformationProcessor;
    }

    @Override
    public Mono<String> startTransformation(String context) {
        return Mono.create(sink -> {
            String id = UUID.randomUUID().toString();
            transformationCache.reserve(context, id);
            transformationProcessor.startTransformation(context, id);
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
                                          boolean keepOldVersions) {
        return Mono.create(sink -> {
            transformationValidator.apply(context, transformationId, lastEventToken);
            transformationProcessor.apply(transformationId, keepOldVersions)
                                   .thenAccept(result -> transformationProcessor.complete(transformationId));
            sink.success();
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
