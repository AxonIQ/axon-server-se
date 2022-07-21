/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing;

import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 08/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class DefaultOperationFactory implements OperationFactory {

    private final ProcessorEventPublisher processorEventsSource;

    private final ClientProcessors processors;

    public DefaultOperationFactory(
            ProcessorEventPublisher processorEventsSource,
            ClientProcessors processors) {
        this.processorEventsSource = processorEventsSource;
        this.processors = processors;
    }

    @Override
    public LoadBalancingOperation move(Integer segment, TrackingEventProcessor processor, String source,
                                       String target) {
        return new Move(processor, target, segment);
    }

    private class Move implements LoadBalancingOperation {

        private final TrackingEventProcessor processor;
        private final String target;
        private final Integer segment;

        private Move(TrackingEventProcessor processor, String target, Integer segment) {
            this.processor = processor;
            this.target = target;
            this.segment = segment;
        }

        @Override
        public void perform() {
            stream(processors.spliterator(), false)
                    .filter(new SameProcessor(processor))
                    .filter(p -> !target.equals(p.clientId()))
                    .forEach(p -> processorEventsSource
                            .releaseSegment(processor.context(),
                                            p.clientId(),
                                            processor.name(),
                                            segment,
                                            UUID.randomUUID().toString()));
        }

        @Override
        public String toString() {
            return "Move segment "+ segment+ " to client "+ target;
        }
    }

}
