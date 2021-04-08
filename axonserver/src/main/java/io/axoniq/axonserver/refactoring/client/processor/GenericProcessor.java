/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.client.processor;

import io.axoniq.axonserver.refactoring.client.processor.listener.ClientProcessor;

import java.util.Collection;

/**
 * Generic state representation of an Event Processor for the UI.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class GenericProcessor implements EventProcessor {

    private final String name;
    private final String mode;
    private final Collection<ClientProcessor> clientProcessors;

    /**
     * Instantiate a {@link GenericProcessor}, used to represent the state of an Event Processor in the UI.
     *
     * @param name       a {@link String} defining the processing group name of this Event Processor
     * @param mode       a {@link String} defining the mode of this Event Processor
     * @param processors a {@link Collection} of {@link ClientProcessor}s portraying the state of this Event Processor
     *                   per client it is running on
     */
    GenericProcessor(String name, String mode, Collection<ClientProcessor> processors) {
        this.name = name;
        this.mode = mode;
        this.clientProcessors = processors;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String mode() {
        return mode;
    }

    /**
     * Return a {@link Collection} of {@link ClientProcessor}s portraying the state of this Event Processor per client
     * it is running on.
     *
     * @return a {@link Collection} of {@link ClientProcessor}s portraying the state of this Event Processor per client
     * it is running on
     */
    protected Collection<ClientProcessor> processors() {
        return this.clientProcessors;
    }
}
