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
public class DefaultEventStoreTransformationService implements EventStoreTransformationService {

    private final Transformers transformers;
    private final Logger auditLog;

//    public DefaultEventStoreTransformationService(LocalEventStore localEventStore, EventStoreTransformationRepository repository) {
//        this(new LocalTransformers(localEventStore::eventIterator, repository));
//    }

    public DefaultEventStoreTransformationService(Transformers transformers) {
        this(transformers, LoggerFactory.getLogger("AUDIT." + DefaultEventStoreTransformationService.class.getName()));
    }

    public DefaultEventStoreTransformationService(Transformers transformers, Logger auditLog) {
        this.transformers = transformers;
        this.auditLog = auditLog;
    }

    @Override
    public Flux<Transformation> transformations() {
        // TODO: 12/22/21
        return Flux.empty();
    }

    @Override
    public Mono<String> start(String context, String description,
                              @Nonnull Authentication authentication) {
        auditLog.info("{}@{}: Request to start transformation - {}",
                      username(authentication.username()),
                      sanitize(context),
                      sanitize(description));
        return transformerFor(context).start(description);
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token, long sequence,
                                  @Nonnull Authentication authentication) {
        auditLog.debug("{}@{}: Request to delete event {}",
                       username(authentication.username()),
                       sanitize(context),
                       token);
        return transformerFor(context).deleteEvent(transformationId, token, sequence);
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event,
                                   long sequence, @Nonnull Authentication authentication) {
        auditLog.debug("{}@{}: Request to replace event {}",
                       username(authentication.username()),
                       sanitize(context),
                       token);
        return transformerFor(context).replaceEvent(transformationId, token, event, sequence);
    }

    @Override
    public Mono<Void> cancel(String context, String id, @Nonnull Authentication authentication) {
        auditLog.info("{}@{}: Request to cancel transformation {}",
                      username(authentication.username()),
                      sanitize(context),
                      sanitize(id));
        return transformerFor(context).cancel(id);
    }

    @Override
    public Mono<Void> startApplying(String context, String transformationId, long sequence,
                                    boolean keepOldVersions,
                                    @Nonnull Authentication authentication) {
        auditLog.info("{}@{}: Request to apply transformation {}",
                      username(authentication.username()),
                      sanitize(context),
                      sanitize(transformationId));
        return transformerFor(context).startApplying(transformationId, sequence, keepOldVersions);
    }

    @Override
    public Mono<Void> startRollingBack(String context, String id, @Nonnull Authentication authentication) {
        auditLog.info("{}@{}: Request to rollback transformation {}",
                      username(authentication.username()),
                      sanitize(context),
                      sanitize(id));
        return transformerFor(context).startRollingBack(id);
    }

    @Override
    public Mono<Void> deleteOldVersions(String context, @Nonnull Authentication authentication) {
        auditLog.info("{}@{}: Request to delete old events.",
                      username(authentication.username()),
                      sanitize(context));
        return transformerFor(context).deleteOldVersions();
    }

    private ContextTransformer transformerFor(String context) {
        return transformers.transformerFor(context);
    }
}
