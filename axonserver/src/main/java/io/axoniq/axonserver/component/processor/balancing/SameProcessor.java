/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;

import java.util.function.Predicate;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
 */
public class SameProcessor implements Predicate<ClientProcessor> {

    private final TrackingEventProcessor processor;

    public SameProcessor(TrackingEventProcessor processor) {
        this.processor = processor;
    }

    @Override
    public boolean test(ClientProcessor p) {
        return p.belongsToComponent(processor.component()) &&
                p.belongsToContext(processor.context()) &&
                p.eventProcessorInfo().getProcessorName().equals(processor.name());
    }
}
