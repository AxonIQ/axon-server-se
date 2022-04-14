/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.serializer.Printable;
import io.axoniq.axonserver.transport.rest.json.GenericProcessor;
import io.axoniq.axonserver.transport.rest.json.StreamingProcessor;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.security.Principal;

/**
 * REST endpoint to deal with operations applicable to an Event Processor.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@RestController
@RequestMapping("v1")
public class EventProcessorRestController {

    private final EventProcessorAdminService service;

    /**
     * Instantiate a REST endpoint to open up several Event Processor operations, like start, stop and segment release,
     * to the Axon Server UI.
     *
     * @param service the service that performs the operations
     */
    public EventProcessorRestController(EventProcessorAdminService service) {
        this.service = service;
    }

    @GetMapping("components/{component}/processors")
    public Flux<Printable> componentProcessors(@PathVariable("component") String component,
                                               @Parameter(hidden = true) final Principal principal) {

        return service.eventProcessorsByComponent(component, new PrincipalAuthentication(principal))
                      .map(p -> p.isStreaming() ? new StreamingProcessor(p) : new GenericProcessor(p));
    }

    /**
     * Processes the request to pause a specific event processor.
     *
     * @param processor            the event processor name
     * @param tokenStoreIdentifier the identifier of the token store for the event processor
     * @param principal            the authenticated user
     */
    @PatchMapping("components/{component}/processors/{processor}/pause")
    public void pause(@PathVariable("processor") String processor,
                      @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                      @Parameter(hidden = true) final Principal principal) {
        service.pause(new EventProcessorIdentifier(processor, tokenStoreIdentifier),
                      new PrincipalAuthentication(principal));
    }

    @PatchMapping("components/{component}/processors/{processor}/start")
    public void start(@PathVariable("processor") String processor,
                      @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                      @Parameter(hidden = true) final Principal principal) {
        service.start(new EventProcessorIdentifier(processor, tokenStoreIdentifier),
                      new PrincipalAuthentication(principal));
    }

    @PatchMapping("components/{component}/processors/{processor}/segments/{segment}/move")
    public void moveSegment(@PathVariable("processor") String processor,
                            @PathVariable("segment") int segment,
                            @RequestParam("target") String target,
                            @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                            @Parameter(hidden = true) final Principal principal) {
        service.move(new EventProcessorIdentifier(processor, tokenStoreIdentifier), segment, target,
                     new PrincipalAuthentication(principal));
    }

    /**
     * Split the biggest segment of the Event Processor with the given {@code processorName}.
     *
     * @param processor            a {@link String} specifying the specific Event Processor to split a segment from
     * @param tokenStoreIdentifier a {@link String} specifying the token store identifier of the Event Processor
     */
    @PatchMapping("components/{component}/processors/{processor}/segments/split")
    public void splitSegment(@PathVariable("processor") String processor,
                             @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                             @Parameter(hidden = true) final Principal principal) {
        service.split(new EventProcessorIdentifier(processor, tokenStoreIdentifier),
                      new PrincipalAuthentication(principal));
    }

    /**
     * Merge the smallest two segments of the Event Processor with the given {@code processorName}.
     *
     * @param processor            a {@link String} specifying the specific Event Processor to merge a segment from
     * @param tokenStoreIdentifier a {@link String} specifying the token store identifier of the Event Processor
     */
    @PatchMapping("components/{component}/processors/{processor}/segments/merge")
    public void mergeSegment(@PathVariable("processor") String processor,
                             @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                             @Parameter(hidden = true) final Principal principal) {
        service.merge(new EventProcessorIdentifier(processor, tokenStoreIdentifier),
                      new PrincipalAuthentication(principal));
    }

    /**
     * This method retrieve instances of client application that contains a specific Tracking Event Processor.
     *
     * @param processor            the name of the tracking event processor
     * @param context              the context of the client
     * @param tokenStoreIdentifier the token store identifier of the tracking event processor
     * @return the list of clients in the specified context that run specified tracking event processor
     */
    @GetMapping("/processors/{processor}/clients")
    public Flux<String> getClientInstancesFor(@PathVariable("processor") String processor,
                                              @RequestParam("context") String context,
                                              @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                                              @Parameter(hidden = true) Principal principal) {
        return service.clientsBy(new EventProcessorIdentifier(processor, tokenStoreIdentifier),
                                 new PrincipalAuthentication(principal));
    }
}
