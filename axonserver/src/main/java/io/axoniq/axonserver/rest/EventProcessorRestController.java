/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.processor.ComponentEventProcessors;
import io.axoniq.axonserver.component.processor.EventProcessor;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REST endpoint to deal with operations applicable to an Event Processor.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@RestController
@RequestMapping("v1")
public class EventProcessorRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final ProcessorEventPublisher processorEventsSource;
    private final ClientProcessors eventProcessors;

    /**
     * Instantiate a REST endpoint to open up several Event Processor operations, like start, stop and segment release,
     * to the Axon Server UI.
     *
     * @param processorEventsSource the {@link ProcessorEventPublisher} used to publish specific application
     *                              events for the provided endpoints
     * @param eventProcessors       an {@link Iterable} of {@link io.axoniq.axonserver.component.processor.listener.ClientProcessor}
     *                              instances containing the known status of all the Event Processors
     */
    public EventProcessorRestController(ProcessorEventPublisher processorEventsSource,
                                        ClientProcessors eventProcessors) {
        this.processorEventsSource = processorEventsSource;
        this.eventProcessors = eventProcessors;
    }

    @GetMapping("components/{component}/processors")
    public Iterable<EventProcessor> componentProcessors(@PathVariable("component") String component,
                                                        @ApiIgnore final Principal principal) {
        if (auditLog.isDebugEnabled()) {
            auditLog.debug("[{}] Request to list Event processors in component \"{}\".",
                           AuditLog.username(principal), component);
        }

        return new ComponentEventProcessors(component, eventProcessors);
    }

    @PatchMapping("components/{component}/processors/{processor}/pause")
    public void pause(@PathVariable("component") String component,
                      @PathVariable("processor") String processor,
                      @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                      @ApiIgnore final Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to pause Event processor \"{}\" in component \"{}\".",
                          AuditLog.username(principal), processor, component);
        }
        clientsByEventProcessor(processor, tokenStoreIdentifier)
                .forEach(clientId -> processorEventsSource
                        .pauseProcessorRequest(clientId, processor));
    }

    @PatchMapping("components/{component}/processors/{processor}/start")
    public void start(@PathVariable("component") String component,
                      @PathVariable("processor") String processor,
                      @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                      @ApiIgnore final Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to start Event processor \"{}\" in component \"{}\".",
                          AuditLog.username(principal), processor, component);
        }
        clientsByEventProcessor(processor, tokenStoreIdentifier)
                .forEach(clientId -> processorEventsSource
                        .startProcessorRequest(clientId, processor));
    }

    @PatchMapping("components/{component}/processors/{processor}/segments/{segment}/move")
    public void moveSegment(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @PathVariable("segment") int segment,
                            @RequestParam("target") String target,
                            @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                            @ApiIgnore final Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to move segment {} of event processor \"{}\" in component \"{}\" to \"{}\".",
                          AuditLog.username(principal), segment, processor, component, target);
        }
        clientsByEventProcessor(processor, tokenStoreIdentifier).forEach(clientId -> {
            if (!target.equals(clientId)) {
                processorEventsSource.releaseSegment(clientId, processor, segment);
            }
        });
    }

    /**
     * Split the smallest segment of the Event Processor with the given {@code processorName}.
     *
     * @param component            a {@link String} specifying the component for which this operation should be
     *                             performed
     * @param processorName        a {@link String} specifying the specific Event Processor to split a segment from
     * @param tokenStoreIdentifier a {@link String} specifying the token store identifier of the Event Processor
     */
    @PatchMapping("components/{component}/processors/{processor}/segments/split")
    public void splitSegment(@PathVariable("component") String component,
                             @PathVariable("processor") String processorName,
                             @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                             @ApiIgnore final Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to split segment of event processor \"{}\" in component \"{}\".",
                          AuditLog.username(principal), processorName, component);
        }
        List<String> clientIds = clientsByEventProcessor(processorName, tokenStoreIdentifier);
        processorEventsSource.splitSegment(clientIds, processorName);
    }

    /**
     * Merge the biggest segment of the Event Processor with the given {@code processorName}.
     *
     * @param component            a {@link String} specifying the component for which this operation should be
     *                             performed
     * @param processorName        a {@link String} specifying the specific Event Processor to merge a segment from
     * @param tokenStoreIdentifier a {@link String} specifying the token store identifier of the Event Processor
     */
    @PatchMapping("components/{component}/processors/{processor}/segments/merge")
    public void mergeSegment(@PathVariable("component") String component,
                             @PathVariable("processor") String processorName,
                             @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                             @ApiIgnore final Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to merge segment of event processor \"{}\" in component \"{}\".",
                          AuditLog.username(principal), processorName, component);
        }
        List<String> clientIds = clientsByEventProcessor(processorName, tokenStoreIdentifier);
        processorEventsSource.mergeSegment(clientIds, processorName);
    }


    /**
     * This method retrieve instances of client application that contains a specific Tracking Event Processor.
     *
     * @param processorName        the name of the tracking event processor
     * @param context              the context of the client
     * @param tokenStoreIdentifier the token store identifier of the tracking event processor
     * @return the list of clients in the specified context that run specified tracking event processor
     */
    @GetMapping("/processors/{processor}/clients")
    public Iterable<String> getClientInstancesFor(@PathVariable("processor") String processorName,
                                                  @RequestParam("context") String context,
                                                  @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                                                  @ApiIgnore Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info(
                    "[{}] Request for a list of clients for context=\"{}\" that contains the processor \"{}\" @ \"{}\"",
                    AuditLog.username(principal),
                    context,
                    processorName,
                    tokenStoreIdentifier);
        }

        return clientsByEventProcessor(processorName, tokenStoreIdentifier);
    }

    private List<String> clientsByEventProcessor(
            String processorName,
            String tokenStoreIdentifier) {
        EventProcessorIdentifier identifier = new EventProcessorIdentifier(processorName, tokenStoreIdentifier);

        return Flux.fromIterable(eventProcessors)
                   .filter(p -> identifier.equals(new EventProcessorIdentifier(p)))
                   .map(ClientProcessor::clientId)
                   .collect(Collectors.toList())
                   .block();
    }
}
