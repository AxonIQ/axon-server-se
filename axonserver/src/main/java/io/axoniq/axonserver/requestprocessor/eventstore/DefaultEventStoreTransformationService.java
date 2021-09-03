/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.requestprocessor.eventstore;

import io.axoniq.axonserver.grpc.event.Event;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class DefaultEventStoreTransformationService implements EventStoreTransformationService {

    private final TransformationValidator transformationValidator;
    private final TransformationProcessor transformationProcessor;

    public DefaultEventStoreTransformationService(
            TransformationValidator transformationValidator,
            TransformationProcessor transformationProcessor) {
        this.transformationValidator = transformationValidator;
        this.transformationProcessor = transformationProcessor;
    }

    @Override
    public Mono<String> startTransformation(String context) {
        return Mono.create(sink -> {
            String id = transformationValidator.register(context);
            transformationProcessor.startTransformation(context, id);
            sink.success(id);
        });
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token, long previousToken) {
        return Mono.create(sink -> {
            transformationValidator.validateDeleteEvent(context, transformationId, token, previousToken);
            transformationProcessor.deleteEvent(transformationId, token);
        });
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event,
                                   long previousToken) {
        return Mono.create(sink -> {
            transformationValidator.validateReplaceEvent(context, transformationId, token, previousToken, event);
            transformationProcessor.replaceEvent(transformationId, token, event);
        });
    }

    @Override
    public Mono<Void> cancelTransformation(String context, String id) {
        return Mono.create(sink -> {
            transformationValidator.cancel(context, id);
            transformationProcessor.cancel(id);
        });
    }

    @Override
    public Mono<Void> applyTransformation(String context, String transformationId, long lastEventToken) {
        return Mono.create(sink -> {
            transformationValidator.apply(context, transformationId, lastEventToken);
            transformationProcessor.apply(transformationId);
        });
    }
}
