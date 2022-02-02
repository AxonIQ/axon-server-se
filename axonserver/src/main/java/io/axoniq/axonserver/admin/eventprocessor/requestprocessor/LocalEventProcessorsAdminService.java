/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
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
    private final Flux<ClientProcessor> eventProcessors;

    /**
     * Default implementation of {@link EventProcessorAdminService}.
     *
     * @param processorEventsSource used to propagate the instructions to the proper clients
     * @param eventProcessors       the list of all event processors
     */
    @Autowired
    public LocalEventProcessorsAdminService(
            ProcessorEventPublisher processorEventsSource,
            ClientProcessors eventProcessors) {
        this(processorEventsSource, Flux.fromIterable(eventProcessors));
    }

    public LocalEventProcessorsAdminService(
            ProcessorEventPublisher processorEventsSource,
            Flux<ClientProcessor> eventProcessors) {
        this.processorEventsSource = processorEventsSource;
        this.eventProcessors = eventProcessors;
    }

    @Nonnull
    @Override
    public Flux<String> clientsBy(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        if (auditLog.isInfoEnabled()) {
            auditLog.info(
                    "[{}] Request for a list of clients that contains the processor \"{}\" @ \"{}\"",
                    AuditLog.username(authentication.username()), sanitize(processor), sanitize(tokenStoreIdentifier));
        }
        EventProcessorIdentifier id = new EventProcessorIdentifier(processor, tokenStoreIdentifier);
        return eventProcessors
                .filter(eventProcessor -> id.equals(new EventProcessorIdentifier(eventProcessor)))
                .map(ClientProcessor::clientId)
                .distinct();
    }

    @Nonnull
    @Override
    public Flux<EventProcessor> eventProcessors(@Nonnull Authentication authentication) {
        return eventProcessors.transform(this::group);
    }

    @Nonnull
    @Override
    public Flux<EventProcessor> eventProcessorsByComponent(@Nonnull String component,
                                                           @Nonnull Authentication authentication) {
        if (auditLog.isInfoEnabled()) {
            auditLog.debug("[{}] Request to list Event processors in component \"{}\".",
                           AuditLog.username(authentication.username()), sanitize(component));
        }
        return eventProcessors
                .filterWhen(c -> eventProcessors
                        .filter(clientProcessor -> clientProcessor.belongsToComponent(component))
                        .map(EventProcessorIdentifier::new)
                        .map(ep -> ep.equals(new EventProcessorIdentifier(c)))
                        .reduce(Boolean::logicalOr))
                .transform(this::group);
    }

    private Flux<EventProcessor> group(Flux<ClientProcessor> clientProcessors) {
        return clientProcessors
                .groupBy(EventProcessorIdentifier::new)
                .flatMap(group -> group.collectList()
                                       .map(list -> new DistributedEventProcessor(group.key(), list)));
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
        return eventProcessors
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

        return eventProcessors
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
        return eventProcessors
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
        return eventProcessors
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
        return eventProcessors
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