/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing;

import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

import java.util.function.Predicate;

/**
 * Predicate which checks if a {@link ClientProcessor} belongs to specific event processor
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class SameProcessor implements Predicate<ClientProcessor> {

    private final EventProcessorIdentifier eventProcessorIdentifier;

    /**
     * Creates an instance for the specified {@link TrackingEventProcessor}
     *
     * @param processor the tracking event processor
     */
    public SameProcessor(TrackingEventProcessor processor) {
        this(new EventProcessorIdentifier(processor.name(), processor.context(), processor.tokenStoreIdentifier()
        ));
    }

    /**
     * Creates an instance for the specified context and {@link ClientProcessor}.
     *
     * @param clientProcessor the event processor instance
     */
    public SameProcessor(ClientProcessor clientProcessor) {
        this(new EventProcessorIdentifier(clientProcessor.eventProcessorInfo().getProcessorName(),
                                          clientProcessor.context(),
                                          clientProcessor.eventProcessorInfo().getTokenStoreIdentifier()
        ));
    }


    /**
     * Creates an instance for the specified context and {@link EventProcessorIdentifier}
     *
     * @param eventProcessorIdentifier the identifier of the event processor
     */
    public SameProcessor(EventProcessorIdentifier eventProcessorIdentifier) {
        this.eventProcessorIdentifier = eventProcessorIdentifier;
    }

    /**
     * Checks if the {@link ClientProcessor} belongs to the the correct event processor, verifying that both context and
     * processor name match.
     *
     * @param processor the {@link ClientProcessor} to be tested
     * @return true if the {@link ClientProcessor} belongs to the the correct event processor, false otherwise.
     */
    @Override
    public boolean test(ClientProcessor processor) {
        EventProcessorInfo i = processor.eventProcessorInfo();
        EventProcessorIdentifier id = new EventProcessorIdentifier(i.getProcessorName(),
                                                                   processor.context(),
                                                                   i.getTokenStoreIdentifier()
        );
        return id.equals(eventProcessorIdentifier);
    }
}
