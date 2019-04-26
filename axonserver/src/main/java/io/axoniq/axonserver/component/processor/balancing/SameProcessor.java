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
import java.util.function.Supplier;

/**
 * Predicate which checks if a {@link ClientProcessor} belongs to specific event processor
 * @author Sara Pellegrini
 */
public class SameProcessor implements Predicate<ClientProcessor> {

    private final Supplier<String> context;

    private final Supplier<String> processorName;

    /**
     * Creates an instance with specified {@link TrackingEventProcessor}
     *
     * @param processor the tracking event processor
     */
    public SameProcessor(TrackingEventProcessor processor) {
        this(processor::context, processor::name);
    }

    /**
     * Creates an instance with specified context and processor name.
     *
     * @param context       the context of the event processor
     * @param processorName the name of the event processor
     */
    public SameProcessor(Supplier<String> context, Supplier<String> processorName) {
        this.context = context;
        this.processorName = processorName;
    }

    /**
     * Checks if the {@link ClientProcessor} belongs to the the correct event processor,
     * verifying that both context and processor name match.
     *
     * @param processor the {@link ClientProcessor} to be tested
     * @return true if the {@link ClientProcessor} belongs to the the correct event processor, false otherwise.
     */
    @Override
    public boolean test(ClientProcessor processor) {
        return processor.belongsToContext(context.get()) &&
                processor.eventProcessorInfo().getProcessorName().equals(processorName.get());
    }
}
