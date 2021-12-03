/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json;

/**
 * Generic state representation of an Event Processor for the UI.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class GenericProcessor implements EventProcessor {

    private final io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor eventProcessor;

    /**
     * Instantiate a {@link GenericProcessor}, used to represent the state of an Event Processor in the UI.
     *
     * @param eventProcessor the event processor state
     */
    public GenericProcessor(io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    @Override
    public String name() {
        return eventProcessor.id().name();
    }

    @Override
    public String mode() {
        return eventProcessor.mode();
    }
}
