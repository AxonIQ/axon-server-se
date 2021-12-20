/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

import static io.axoniq.axonserver.util.StringUtils.sanitize;

/**
 * Service that implements the operations applicable to an Event Processor.
 *
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
@Service
public class LocalEventProcessorsAdminService implements EventProcessorAdminService {

    private static final Logger auditLog = AuditLog.getLogger();
    private final ProcessorEventPublisher processorEventsSource;
    private final ClientProcessors eventProcessors;

    /**
     * Default implementation of {@link EventProcessorAdminService}.
     *
     * @param processorEventsSource used to propagate the instructions to the proper clients
     * @param eventProcessors       the list of all event processors
     */
    public LocalEventProcessorsAdminService(
            ProcessorEventPublisher processorEventsSource,
            ClientProcessors eventProcessors) {
        this.processorEventsSource = processorEventsSource;
        this.eventProcessors = eventProcessors;
    }

    @Nonnull
    @Override
    public Mono<Void> pause(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to pause Event processor \"{}@{}\".",
                          AuditLog.username(authentication.username()),
                          sanitize(processor),
                          sanitize(identifier.tokenStoreIdentifier()));
        }
        return Flux.fromIterable(eventProcessors)
                   .filter(eventProcessor -> new EventProcessorIdentifier(eventProcessor).equals(identifier))
                   .doOnNext(ep -> processorEventsSource.pauseProcessorRequest(ep.context(), ep.clientId(), processor))
                   .then();
        // the context will be removed from the event processor
    }

    @Nonnull
    @Override
    public Mono<Void> start(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to start Event processor \"{}@{}\".",
                          AuditLog.username(authentication.username()),
                          sanitize(processor),
                          sanitize(identifier.tokenStoreIdentifier()));
        }

        return Flux.fromIterable(eventProcessors)
                   .filter(eventProcessor -> new EventProcessorIdentifier(eventProcessor).equals(identifier))
                   .doOnNext(ep -> processorEventsSource.startProcessorRequest(ep.context(), ep.clientId(), processor))
                   .then();
        // the context will be removed from the event processor
    }

    @Nonnull
    @Override
    public Mono<Void> split(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to split a segment of Event processor \"{}@{}\".",
                          AuditLog.username(authentication.username()),
                          sanitize(processor),
                          sanitize(tokenStoreIdentifier));
        }

        EventProcessorIdentifier id = new EventProcessorIdentifier(processor, tokenStoreIdentifier);
        return Flux.fromIterable(eventProcessors)
                   .filter(eventProcessor -> id.equals(new EventProcessorIdentifier(eventProcessor)))
                   .groupBy(ClientProcessor::context)
                   .flatMap(contextGroup -> contextGroup
                           .map(ClientProcessor::clientId)
                           .collectList()
                           .doOnNext(clients -> processorEventsSource.splitSegment(contextGroup.key(),
                                                                                   clients,
                                                                                   processor)))
                   .then();
        // the context will be removed from the event processor
    }

    @Nonnull
    @Override
    public Mono<Void> merge(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to merge two segments of Event processor \"{}@{}\".",
                          AuditLog.username(authentication.username()),
                          sanitize(processor),
                          sanitize(tokenStoreIdentifier));
        }

        EventProcessorIdentifier id = new EventProcessorIdentifier(processor, tokenStoreIdentifier);
        return Flux.fromIterable(eventProcessors)
                   .filter(eventProcessor -> id.equals(new EventProcessorIdentifier(eventProcessor)))
                   .groupBy(ClientProcessor::context)
                   .flatMap(contextGroup -> contextGroup
                           .map(ClientProcessor::clientId)
                           .collectList()
                           .doOnNext(clients -> processorEventsSource.mergeSegment(contextGroup.key(),
                                                                                   clients,
                                                                                   processor)))
                   .then();
        // the context will be removed from the event processor
    }

    /**
     * Handles a request to move a certain segment for a certain event processor to a specific client.
     * The method returns once the request has been propagated to the proper clients.
     * It doesn't imply that the segment has been moved already.
     *
     * @param identifier     the event processor identifier
     * @param segment        the segment to move
     * @param target         the client that should claim the segment
     * @param authentication info about the authenticated user
     */
    @Nonnull
    @Override
    public Mono<Void> move(@Nonnull EventProcessorId identifier, int segment, @Nonnull String target,
                           @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to move the segment {} for Event processor \"{}@{}\" to client {}.",
                          AuditLog.username(authentication.username()),
                          segment,
                          sanitize(processor),
                          sanitize(tokenStoreIdentifier),
                          sanitize(target));
        }

        EventProcessorIdentifier id = new EventProcessorIdentifier(processor, tokenStoreIdentifier);
        return Flux.fromIterable(eventProcessors)
                   .filter(eventProcessor -> id.equals(new EventProcessorIdentifier(eventProcessor)))
                   .filter(eventProcessor -> !target.equals(eventProcessor.clientId()))
                   .doOnNext(ep -> processorEventsSource.releaseSegment(ep.context(),
                                                                        ep.clientId(),
                                                                        processor,
                                                                        segment))
                   .then();
        // the context will be removed from the event processor
    }
}