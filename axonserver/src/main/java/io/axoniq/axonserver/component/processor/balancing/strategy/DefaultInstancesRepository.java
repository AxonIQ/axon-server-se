/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.SameProcessor;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 10/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class DefaultInstancesRepository implements ThreadNumberBalancing.InstancesRepo {

    private final ClientProcessors processors;

    DefaultInstancesRepository(ClientProcessors processors) {
        this.processors = processors;
    }

    @Override
    public Iterable<ThreadNumberBalancing.Application> findFor(TrackingEventProcessor processor) {
        return () -> stream(processors.spliterator(), false)
                .filter(new SameProcessor(processor))
                .filter(ClientProcessor::running)
                .map(p -> {
                    EventProcessorInfo i = p.eventProcessorInfo();
                    int threadPoolSize = i.getAvailableThreads() + i.getSegmentStatusCount();
                    List<SegmentStatus> trackers = i.getSegmentStatusList();
                    Set<Integer> segments = trackers.stream().map(SegmentStatus::getSegmentId).collect(toSet());
                    return new ThreadNumberBalancing.Application(p.clientId(), threadPoolSize, segments);
                }).iterator();
    }

}
