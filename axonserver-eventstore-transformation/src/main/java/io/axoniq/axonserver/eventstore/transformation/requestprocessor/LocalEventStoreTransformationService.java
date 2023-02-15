/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.apply.TransformationApplyTask;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationCleanTask;
import io.axoniq.axonserver.eventstore.transformation.compact.EventStoreCompactionTask;
import io.axoniq.axonserver.eventstore.transformation.spi.TransformationAllowed;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

import static io.axoniq.axonserver.util.StringUtils.sanitize;
import static io.axoniq.axonserver.util.StringUtils.username;

/**
 * Implementation of the {@link EventStoreTransformationService}.
 * <p>
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public class LocalEventStoreTransformationService implements EventStoreTransformationService {

    private final Transformers transformers;
    private final Transformations transformations;
    private final Logger auditLog;
    private final TransformationApplyTask transformationApplyTask;
    private final EventStoreCompactionTask transformationRollBackTask;
    private final TransformationCleanTask transformationCleanTask;
    private final TransformationAllowed transformationAllowed;

    public void init() {
        transformationApplyTask.start();
        transformationRollBackTask.start();
        transformationCleanTask.start();
    }

    public void destroy() {
        transformationApplyTask.stop();
        transformationRollBackTask.stop();
        transformationCleanTask.stop();
    }

    public LocalEventStoreTransformationService(Transformers transformers, Transformations transformations,
                                                EventStoreCompactionTask transformationRollBackTask,
                                                TransformationApplyTask transformationApplyTask,
                                                TransformationCleanTask transformationCleanTask,
                                                TransformationAllowed transformationAllowed) {
        this(transformers,
             transformations,
             LoggerFactory.getLogger("AUDIT." + LocalEventStoreTransformationService.class.getName()),
             transformationApplyTask, transformationRollBackTask, transformationCleanTask,
             transformationAllowed);
    }

    public LocalEventStoreTransformationService(Transformers transformers, Transformations transformations,
                                                Logger auditLog,
                                                TransformationApplyTask transformationApplyTask,
                                                EventStoreCompactionTask transformationRollBackTask,
                                                TransformationCleanTask transformationCleanTask,
                                                TransformationAllowed transformationAllowed) {
        this.transformers = transformers;
        this.transformations = transformations;
        this.auditLog = auditLog;
        this.transformationApplyTask = transformationApplyTask;
        this.transformationRollBackTask = transformationRollBackTask;
        this.transformationCleanTask = transformationCleanTask;
        this.transformationAllowed = transformationAllowed;
    }

    @Override
    public Flux<Transformation> transformations(String context, @Nonnull Authentication authentication) {
        return transformations.allTransformations()
                              .filter(t -> context.equals(t.context()))
                              .doFirst(() -> auditLog.info("{}: Request to list transformations",
                                                           username(authentication.username())));
    }

    @Override
    public Mono<Void> start(String id, String context, String description,
                            @Nonnull Authentication authentication) {
        return transformationAllowed.validate(context)
                                    .then(transformerFor(context).flatMap(transformer -> transformer.start(id,
                                                                                                           description))
                                                                 .doFirst(() -> auditLog.info(
                                                                         "{}@{}: Request to start transformation - {}",
                                                                         username(authentication.username()),
                                                                         sanitize(context),
                                                                         sanitize(description)))
                                                                 .doOnError(t -> auditLog.error(
                                                                         "Transformation {}: '{}' couldn't be started for context {}.",
                                                                         id,
                                                                         description,
                                                                         context,
                                                                         t)));
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token, long sequence,
                                  @Nonnull Authentication authentication) {
        return transformerFor(context).flatMap(transformer -> transformer.deleteEvent(transformationId,
                                                                                      token,
                                                                                      sequence))
                                      .doFirst(() -> auditLog.info("{}@{}: Request to delete event {}",
                                                                   username(authentication.username()),
                                                                   sanitize(context),
                                                                   token));
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event,
                                   long sequence, @Nonnull Authentication authentication) {
        return transformerFor(context).flatMap(transformer -> transformer.replaceEvent(transformationId,
                                                                                       token,
                                                                                       event,
                                                                                       sequence))
                                      .doFirst(() -> auditLog.info("{}@{}: Request to replace event {}",
                                                                   username(authentication.username()),
                                                                   sanitize(context),
                                                                   token));
    }

    @Override
    public Mono<Void> cancel(String context, String id, @Nonnull Authentication authentication) {
        return transformerFor(context).flatMap(transformer -> transformer.cancel(id))
                                      .doFirst(() -> auditLog.info("{}@{}: Request to cancel transformation {}",
                                                                   username(authentication.username()),
                                                                   sanitize(context),
                                                                   sanitize(id)));
    }

    @Override
    public Mono<Void> startApplying(String context, String transformationId, long sequence,
                                    @Nonnull Authentication authentication) {
        String applier = username(authentication.username());
        return transformationAllowed.validate(context)
                                    .then(transformerFor(context).flatMap(transformer -> transformer.startApplying(
                                                                         transformationId,
                                                                         sequence,
                                                                         applier))
                                                                 .doFirst(() -> auditLog.info(
                                                                         "{}@{}: Request to apply transformation {}",
                                                                         applier,
                                                                         sanitize(context),
                                                                         sanitize(transformationId))));
    }

    @Override
    public Mono<Void> compact(String compactionId, String context, @Nonnull Authentication authentication) {
        return transformationAllowed.validate(context)
                                    .then(transformerFor(context).flatMap(contextTransformer -> contextTransformer.compact(
                                                                         compactionId))
                                                                 .doFirst(() -> auditLog.info(
                                                                         "{}@{}: Request to delete old events.",
                                                                         username(authentication.username()),
                                                                         sanitize(context))));
    }

    private Mono<ContextTransformer> transformerFor(String context) {
        return transformers.transformerFor(context);
    }
}
