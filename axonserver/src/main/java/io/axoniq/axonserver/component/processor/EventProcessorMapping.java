/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Mapper to construct an {@link EventProcessor} based on a given {@link ClientProcessor}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class EventProcessorMapping implements BiFunction<String, Collection<ClientProcessor>, EventProcessor> {

    private static final String TRACKING_EVENT_PROCESSOR_MODE = "Tracking";

    @Override
    public EventProcessor apply(String name, Collection<ClientProcessor> clientProcessors) {
        String mode = modeOf(clientProcessors);
        if (TRACKING_EVENT_PROCESSOR_MODE.equals(mode) || isStreamingProcessor(clientProcessors)) {
            return new StreamingProcessor(name, mode, clientProcessors);
        } else {
            return new GenericProcessor(name, mode, clientProcessors);
        }
    }

    private String modeOf(Collection<ClientProcessor> clientProcessors) {
        Set<String> modes = new HashSet<>();
        for (ClientProcessor clientProcessor : clientProcessors) {
            modes.add(clientProcessor.eventProcessorInfo().getMode());
        }
        return modes.size() == 1 ? modes.iterator().next() : "Multiple processing mode detected";
    }

    private boolean isStreamingProcessor(Collection<ClientProcessor> clientProcessors) {
        return clientProcessors.stream()
                               .findFirst()
                               .map(ClientProcessor::eventProcessorInfo)
                               .map(EventProcessorInfo::getIsStreamingProcessor)
                               .orElse(false);
    }
}
