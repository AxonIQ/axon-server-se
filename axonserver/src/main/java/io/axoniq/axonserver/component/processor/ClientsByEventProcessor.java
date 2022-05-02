/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

import static java.util.stream.StreamSupport.stream;

/**
 * Iterable of clientStreamIdentifiers that contain the processor defined by {@link EventProcessorIdentifier}
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public class ClientsByEventProcessor implements Iterable<String> {

    private final ClientProcessors eventProcessors;

    /**
     * Creates an instance of {@link ClientsByEventProcessor} based on  the iterable of all
     * registered {@link ClientProcessor}s, the specified context and the specified
     * {@link EventProcessorIdentifier}.
     *
     * @param processorId         the identifier of the event processor we are interested in
     * @param allClientProcessors all the {@link ClientProcessor}s instances of connected clients
     */
    public ClientsByEventProcessor(EventProcessorIdentifier processorId,
                                   ClientProcessors allClientProcessors) {
        this.eventProcessors = new ClientProcessorsByIdentifier(allClientProcessors, processorId);
    }


    /**
     * Creates an instance of {@link ClientsByEventProcessor} based on
     * the iterable of all registered {@link ClientProcessor}s belonging to the event processor we are interested in and
     * the specified context.
     *
     * @param eventProcessors all the {@link ClientProcessor}s belonging to the event processor we are interested in
     */
    public ClientsByEventProcessor(ClientProcessors eventProcessors) {
        this.eventProcessors = eventProcessors;
    }

    @NotNull
    @Override
    public Iterator<String> iterator() {
        return stream(eventProcessors.spliterator(), false)
                .map(ClientProcessor::clientId)
                .iterator();
    }
}
