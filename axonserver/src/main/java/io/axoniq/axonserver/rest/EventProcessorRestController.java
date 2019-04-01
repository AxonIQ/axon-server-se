/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.processor.ComponentProcessors;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.jetbrains.annotations.NotNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * REST endpoint to deal with operations applicable to an Event Processor.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@RestController
@RequestMapping("v1")
public class EventProcessorRestController {

    private final ProcessorEventPublisher processorEventsSource;
    private final ClientProcessors eventProcessors;
    private final Clients clients;

    /**
     * Instantiate a REST endpoint to open up several Event Processor operations, like start, stop and segment release,
     * to the Axon Server UI.
     *
     * @param processorEventsSource the {@link ProcessorEventPublisher} used to publish specific application
     *                              events for the provided endpoints
     * @param eventProcessors       an {@link Iterable} of {@link io.axoniq.axonserver.component.processor.listener.ClientProcessor}
     *                              instances containing the known status of all the Event Processors
     * @param clients               an {@link Iterable} of {@link Client} instances, used to publish an application
     *                              event to each Client which should receive it
     */
    public EventProcessorRestController(ProcessorEventPublisher processorEventsSource,
                                        ClientProcessors eventProcessors,
                                        Clients clients) {
        this.processorEventsSource = processorEventsSource;
        this.eventProcessors = eventProcessors;
        this.clients = clients;
    }

    @GetMapping("components/{component}/processors")
    public Iterable componentProcessors(@PathVariable("component") String component,
                                        @RequestParam("context") String context) {
        return new ComponentProcessors(component, context, eventProcessors);
    }

    @PatchMapping("components/{component}/processors/{processor}/pause")
    public void pause(@PathVariable("component") String component,
                      @PathVariable("processor") String processor,
                      @RequestParam("context") String context) {
        clientIterable(component, context)
                .forEach(client -> processorEventsSource.pauseProcessorRequest(client.name(), processor));
    }

    @PatchMapping("components/{component}/processors/{processor}/start")
    public void start(@PathVariable("component") String component,
                      @PathVariable("processor") String processor,
                      @RequestParam("context") String context) {
        clientIterable(component, context)
                .forEach(client -> processorEventsSource.startProcessorRequest(client.name(), processor));
    }

    @PatchMapping("components/{component}/processors/{processor}/segments/{segment}/move")
    public void moveSegment(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @PathVariable("segment") int segment,
                            @RequestParam("target") String target,
                            @RequestParam("context") String context) {
        clientIterable(component, context).forEach(client -> {
            if (!target.equals(client.name())) {
                processorEventsSource.releaseSegment(client.name(), processor, segment);
            }
        });
    }

    /**
     * Split the smallest segment of the Event Processor with the given {@code processorName}.
     *
     * @param component     a {@link String} specifying the component for which this operation should be performed
     * @param processorName a {@link String} specifying the specific Event Processor to split a segment from
     * @param context       a {@link String} defining the context within which this operation should occur
     */
    @PatchMapping("components/{component}/processors/{processor}/segments/split")
    public void splitSegment(@PathVariable("component") String component,
                             @PathVariable("processor") String processorName,
                             @RequestParam("context") String context) {
        List<String> clientNames = StreamSupport.stream(clientIterable(component, context).spliterator(), false)
                                                .map(Client::name)
                                                .collect(Collectors.toList());
        processorEventsSource.splitSegment(clientNames, processorName);
    }

    /**
     * Merge the biggest segment of the Event Processor with the given {@code processorName}.
     *
     * @param component     a {@link String} specifying the component for which this operation should be performed
     * @param processorName a {@link String} specifying the specific Event Processor to merge a segment from
     * @param context       a {@link String} defining the context within which this operation should occur
     */
    @PatchMapping("components/{component}/processors/{processor}/segments/merge")
    public void mergeSegment(@PathVariable("component") String component,
                             @PathVariable("processor") String processorName,
                             @RequestParam("context") String context) {
        List<String> clientNames = StreamSupport.stream(clientIterable(component, context).spliterator(), false)
                                                .map(Client::name)
                                                .collect(Collectors.toList());
        processorEventsSource.mergeSegment(clientNames, processorName);
    }

    @NotNull
    private ComponentItems<Client> clientIterable(String component, String context) {
        return new ComponentItems<>(component, context, clients);
    }
}
