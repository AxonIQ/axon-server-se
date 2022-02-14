/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.Instruction;
import io.axoniq.axonserver.admin.InstructionCache;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ClientContext;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.util.ConstraintCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
    private final Logger logger = LoggerFactory.getLogger(LocalEventProcessorsAdminService.class);
    private final ProcessorEventPublisher processorEventsSource;
    private final Flux<ClientProcessor> eventProcessors;
    private final ConstraintCache<String, Instruction> instructionCache;

    /**
     * Default implementation of {@link EventProcessorAdminService}.
     *
     * @param processorEventsSource used to propagate the instructions to the proper clients
     * @param eventProcessors       the list of all event processors
     */
    @Autowired
    public LocalEventProcessorsAdminService(
            ProcessorEventPublisher processorEventsSource,
            ClientProcessors eventProcessors,
            InstructionCache instructionCache) {
        this(processorEventsSource, Flux.fromIterable(eventProcessors), instructionCache);
    }

    public LocalEventProcessorsAdminService(
            ProcessorEventPublisher processorEventsSource,
            Flux<ClientProcessor> eventProcessors,
            ConstraintCache<String, Instruction> instructionCache) {
        this.processorEventsSource = processorEventsSource;
        this.eventProcessors = eventProcessors;
        this.instructionCache = instructionCache;
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
        String requestDescription = "Pause " + processor;
        return eventProcessors
                .doFirst(() -> {
                    if (auditLog.isInfoEnabled()) {
                        auditLog.info("[{}] Request to pause Event processor \"{}@{}\".",
                                      AuditLog.username(authentication.username()),
                                      sanitize(processor),
                                      sanitize(identifier.tokenStoreIdentifier()));
                    }
                })
                .filter(eventProcessor -> new EventProcessorIdentifier(eventProcessor).equals(identifier))
                .collectList()
                .flatMap(clients -> Mono.<Void>create(sink -> {
                    if (clients.isEmpty()) {
                        sink.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND,
                                                                  "Event processor not found"));
                        return;
                    }

                    String instructionId = UUID.randomUUID().toString();
                    Set<String> targetClients = clients.stream()
                                                       .map(ClientProcessor::clientId)
                                                       .collect(Collectors.toSet());

                    instructionCache.put(instructionId, new InstructionInformation(sink,
                                                                                   instructionId,
                                                                                   requestDescription,
                                                                                   targetClients));
                    clients.forEach(ep -> processorEventsSource.pauseProcessorRequest(ep.context(),
                                                                                      ep.clientId(),
                                                                                      processor,
                                                                                      instructionId));
                }))
                .doOnError(err -> logError(requestDescription, err));
    }

    private void logError(String description, Throwable err) {
        logger.warn("{} failed", description, err);
    }

    @Nonnull
    @Override
    public Mono<Void> start(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String requestDescription = "Start " + processor;
        return eventProcessors
                .doFirst(() -> {
                    if (auditLog.isInfoEnabled()) {
                        auditLog.info("[{}] Request to start Event processor \"{}@{}\".",
                                      AuditLog.username(authentication.username()),
                                      sanitize(processor),
                                      sanitize(identifier.tokenStoreIdentifier()));
                    }
                })
                .filter(eventProcessor -> new EventProcessorIdentifier(eventProcessor).equals(identifier))
                .collectList()
                .flatMap(clients -> Mono.<Void>create(sink -> {
                    if (clients.isEmpty()) {
                        sink.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND,
                                                                  "Event processor not found"));
                        return;
                    }

                    String instructionId = UUID.randomUUID().toString();
                    Set<String> targetClients = clients.stream()
                                                       .map(ClientProcessor::clientId)
                                                       .collect(Collectors.toSet());
                    instructionCache.put(instructionId, new InstructionInformation(sink,
                                                                                   instructionId,
                                                                                   requestDescription,
                                                                                   targetClients));
                    clients.forEach(ep -> processorEventsSource.startProcessorRequest(ep.context(),
                                                                                      ep.clientId(),
                                                                                      processor,
                                                                                      instructionId));
                }))
                .doOnError(err -> logError(requestDescription, err));
    }

    @Nonnull
    @Override
    public Mono<Void> split(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String requestDescription = "Split " + processor;
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        return eventProcessors
                .doFirst(() -> {
                    if (auditLog.isInfoEnabled()) {
                        auditLog.info("[{}] Request to split a segment of Event processor \"{}@{}\".",
                                      AuditLog.username(authentication.username()),
                                      sanitize(processor),
                                      sanitize(tokenStoreIdentifier));
                    }
                })
                .filter(eventProcessor -> new EventProcessorIdentifier(eventProcessor).equals(identifier))
                .flatMap(clientProcessor -> Flux.fromIterable(clientProcessor.eventProcessorInfo()
                                                                             .getSegmentStatusList())
                                                .map(segmentStatus -> new Segment(clientProcessor.clientId(),
                                                                                  clientProcessor.context(),
                                                                                  segmentStatus.getSegmentId(),
                                                                                  segmentStatus.getOnePartOf())))
                .reduce((segment, segment2) -> segment.onePartOf < segment2.onePartOf ? segment : segment2)
                .switchIfEmpty(Mono.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND,
                                                                         "Event processor not found")))
                .flatMap(largestSegment -> Mono.<Void>create(sink -> {
                    String instructionId = UUID.randomUUID().toString();
                    instructionCache.put(instructionId, new InstructionInformation(sink,
                                                                                   instructionId,
                                                                                   requestDescription,
                                                                                   Collections.singleton(largestSegment.clientId)));
                    processorEventsSource.splitSegment(largestSegment.context,
                                                       largestSegment.clientId,
                                                       processor,
                                                       largestSegment.segmentId,
                                                       instructionId);
                }))
                .doOnError(err -> logError(requestDescription, err));
    }

    @Nonnull
    @Override
    public Mono<Void> merge(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String requestDescription = "Merge " + processor;
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        return eventProcessors
                .doFirst(() -> {
                    if (auditLog.isInfoEnabled()) {
                        auditLog.info("[{}] Request to merge a segment of Event processor \"{}@{}\".",
                                      AuditLog.username(authentication.username()),
                                      sanitize(processor),
                                      sanitize(tokenStoreIdentifier));
                    }
                })
                .filter(eventProcessor -> new EventProcessorIdentifier(eventProcessor).equals(identifier))
                .flatMap(clientProcessor -> Flux.fromIterable(clientProcessor.eventProcessorInfo()
                                                                             .getSegmentStatusList())
                                                .map(segmentStatus -> new Segment(clientProcessor.clientId(),
                                                                                  clientProcessor.context(),
                                                                                  segmentStatus.getSegmentId(),
                                                                                  segmentStatus.getOnePartOf())))
                .collectList()
                .flatMap(segments -> Mono.<Void>create(sink -> {
                    String instructionId = UUID.randomUUID().toString();
                    Segment smallestSegment = segments.stream()
                                                      .reduce((s1, s2) -> s1.onePartOf > s2.onePartOf ? s1 : s2)
                                                      .orElseThrow(() -> new IllegalArgumentException(
                                                              "No segments found for processor name [" + processor
                                                                      + "]"));
                    int segmentId = deduceSegmentToMerge(smallestSegment);

                    // release segmentId
                    segments.stream()
                            .map(s -> new ClientContext(s.clientId, s.context))
                            .distinct()
                            .filter(c -> !smallestSegment.clientId.equals(c.clientId()))
                            .forEach(clientContext -> processorEventsSource.releaseSegment(clientContext.context(),
                                                                                           clientContext.clientId(),
                                                                                           processor,
                                                                                           segmentId,
                                                                                           instructionId));


                    instructionCache.put(instructionId, new InstructionInformation(sink,
                                                                                   instructionId,
                                                                                   requestDescription,
                                                                                   Collections.singleton(smallestSegment.clientId)));

                    processorEventsSource.mergeSegment(smallestSegment.context, smallestSegment.clientId,
                                                       processor,
                                                       smallestSegment.segmentId,
                                                       instructionId);
                }))
                .doOnError(err -> logError(requestDescription, err));
    }

    @Nonnull
    @Override
    public Mono<Void> move(@Nonnull EventProcessorId identifier, int segment, @Nonnull String target,
                           @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        String requestDescription = "Move " + processor;
        EventProcessorIdentifier id = new EventProcessorIdentifier(processor, tokenStoreIdentifier);

        return eventProcessors
                .doFirst(() -> {
                    if (auditLog.isInfoEnabled()) {
                        auditLog.info("[{}] Request to start Event processor \"{}@{}\".",
                                      AuditLog.username(authentication.username()),
                                      sanitize(processor),
                                      sanitize(identifier.tokenStoreIdentifier()));
                    }
                })
                .filter(eventProcessor -> id.equals(new EventProcessorIdentifier(eventProcessor)))
                .doOnNext(eventProcessor -> {
                    if (target.equals(eventProcessor.clientId())) {
                        if (eventProcessor.eventProcessorInfo().getAvailableThreads() -
                                eventProcessor.eventProcessorInfo().getActiveThreads() < 1) {
                            throw new MessagingPlatformException(ErrorCode.OTHER, "No available threads on target");
                        }
                    }
                })
                .filter(eventProcessor -> !target.equals(eventProcessor.clientId()))
                .collectList()
                .flatMap(clients -> Mono.<Void>create(sink -> {
                    // only one client which already should have this segment or should be able to claim it
                    if (clients.isEmpty()) {
                        sink.success();
                    }
                    String instructionId = UUID.randomUUID().toString();
                    Set<String> targetClients = clients.stream()
                                                       .map(ClientProcessor::clientId)
                                                       .collect(Collectors.toSet());
                    instructionCache.put(instructionId, new InstructionInformation(sink,
                                                                                   instructionId,
                                                                                   requestDescription,
                                                                                   targetClients));
                    clients.forEach(ep -> processorEventsSource.releaseSegment(ep.context(),
                                                                               ep.clientId(),
                                                                               processor,
                                                                               segment,
                                                                               instructionId));
                }))
                // TODO: 11/02/2022 check if target client actually has the segment
                .doOnError(err -> logError(requestDescription, err));
    }

    private int deduceSegmentToMerge(Segment segment) {
        int segmentSize = segment.onePartOf;
        int siblingMask = segmentSize >>> 1; // mask to determine sibling segment
        return segment.segmentId ^ siblingMask;
    }

    private static class Segment {

        private final String clientId;
        private final String context;
        private final int segmentId;
        private final int onePartOf;

        private Segment(String clientId, String context, int segmentId, int onePartOf) {
            this.clientId = clientId;
            this.context = context;
            this.segmentId = segmentId;
            this.onePartOf = onePartOf;
        }
    }
}