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

    public void cancelTransformation(String context) {
//        transformationValidator.get(context);
//        TransformationDescription transformation = activeTransformations.remove(context);
//        if (transformation != null) {
//            transformation.cancel();
//        }
    }

    public void applyTransformation(String context) {
//        TransformationDescription transformation = activeTransformations.get(context);
//        if (transformation != null) {
//            transformation.apply(context);
//        }
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token, long previousToken) {
        return Mono.create(sink -> {
            transformationValidator.registerDeleteEvent(context, transformationId, token, previousToken);
            transformationProcessor.deleteEvent(transformationId, token);
        });
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event,
                                   long previousToken) {
        return Mono.create(sink -> {
            transformationValidator.registerReplaceEvent(context, transformationId, token, previousToken, event);
            transformationProcessor.replaceEvent(transformationId, token, event);
        });
    }

    @Override
    public Mono<Void> cancelTransformation(String context, String id) {
        return Mono.empty();
//        return transformation(context, id).mapNotNull(t -> {
//            t.cancel();
//            activeTransformations.remove(context);
//            return null;
//        });
    }

    @Override
    public Mono<Void> applyTransformation(String context, String transformationId, long lastEventToken, long lastSnapshotToken) {
        return Mono.create(sink -> {
            transformationProcessor.apply(transformationId);
        });
    }
}
