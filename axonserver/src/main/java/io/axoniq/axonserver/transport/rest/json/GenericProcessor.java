/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json;

import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;
import io.axoniq.axonserver.transport.rest.json.warning.ActiveWarnings;
import io.axoniq.axonserver.transport.rest.json.warning.Warning;

import static java.util.Collections.emptyList;

/**
 * Generic state representation of an Event Processor for the UI.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class GenericProcessor implements Printable {

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
    public void printOn(Media media) {
        media.with("name", name())
             .with("context", eventProcessor.id().context())
             .with("mode", eventProcessor.mode())
             .with("isStreaming", eventProcessor.isStreaming())
             .with("fullName", fullName())
             .with("warnings", new ActiveWarnings(warnings()));
    }

    String name() {
        return eventProcessor.id().name();
    }

    String fullName() {
        return name();
    }

    Iterable<Warning> warnings() {
        return emptyList();
    }
}
