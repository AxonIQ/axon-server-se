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
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationProcessor;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationStateManager;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationValidator;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

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
    private final Map<String, Scheduler> executorServicePerContext = new ConcurrentHashMap<>();
    private final Logger auditLog = AuditLog.getLogger();

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
    public Mono<String> startTransformation(String context, String description,
                                            @Nonnull Authentication authentication) {
        return Mono.<String>create(sink -> {
            auditLog.info("{}@{}: Request to start transformation - {}",
                          authentication.username(),
                          context,
                          description);
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
        }).subscribeOn(scheduler(context));
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token, long previousToken,
                                  @Nonnull Authentication authentication) {
        return Mono.defer(() -> {
            auditLog.debug("{}@{}: Request to delete event {}", authentication.username(), context, token);

            transformationValidator.validateDeleteEvent(context, transformationId, token, previousToken);
            return transformationProcessor.deleteEvent(transformationId, token);
        }).subscribeOn(scheduler(context));
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event,
                                   long previousToken, @Nonnull Authentication authentication) {
        return Mono.defer(() -> {
            auditLog.debug("{}@{}: Request to replace event {}", authentication.username(), context, token);
            transformationValidator.validateReplaceEvent(context, transformationId, token, previousToken, event);
            return transformationProcessor.replaceEvent(transformationId, token, event);
        }).subscribeOn(scheduler(context));
    }

    @Override
    public Mono<Void> cancelTransformation(String context, String id, @Nonnull Authentication authentication) {
        return Mono.<Void>fromRunnable(() -> {
            auditLog.info("{}@{}: Request to cancel transformation {}", authentication.username(), context, id);
            transformationValidator.cancel(context, id);
            transformationProcessor.cancel(id);
        }).subscribeOn(scheduler(context));
    }

    @Override
    public Mono<Void> applyTransformation(String context, String transformationId, long lastEventToken,
                                          boolean keepOldVersions,
                                          @Nonnull Authentication authentication) {
        return Mono.defer(() -> {
            auditLog.info("{}@{}: Request to apply transformation {}",
                          authentication.username(),
                          context,
                          transformationId);
            transformationValidator.apply(context, transformationId, lastEventToken);
            return transformationStateManager.firstToken(transformationId)
                                             .flatMap(firstToken -> transformationProcessor.apply(
                                                     transformationId,
                                                     localEventStore.keepOldVersions(context) || keepOldVersions,
                                                     authentication.username(),
                                                     new Date(),
                                                     firstToken,
                                                     lastEventToken))
                                             .doOnSuccess(r -> transformationProcessor.complete(transformationId));
        }).subscribeOn(scheduler(context));
    }

    @Override
    public Mono<Void> rollbackTransformation(String context, String id, @Nonnull Authentication authentication) {
        return Mono.<Void>fromRunnable(() -> {
            auditLog.info("{}@{}: Request to rollback transformation {}", authentication.username(), context, id);
            transformationValidator.rollback(context, id);
            transformationProcessor.rollbackTransformation(context, id);
        }).subscribeOn(scheduler(context));
    }

    @Override
    public Mono<Void> deleteOldVersions(String context, String id, @Nonnull Authentication authentication) {
        return Mono.<Void>fromRunnable(() -> {
            auditLog.info("{}@{}: Request to delete old events from transformation {}",
                          authentication.username(),
                          context,
                          id);
            transformationValidator.deleteOldVersions(context, id);
            transformationProcessor.deleteOldVersions(context, id);
        }).subscribeOn(scheduler(context));
    }

    private Scheduler scheduler(String context) {
        return executorServicePerContext.computeIfAbsent(context,
                                                         c -> Schedulers.newSingle(context + "-transformation", true));
    }
}
