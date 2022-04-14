/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing;

import java.util.Collection;
import java.util.Map;

/**
 * Created by Sara Pellegrini on 07/08/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeOperationFactory implements OperationFactory {

    private final Map<String,Collection<Integer>> configuration;

    public FakeOperationFactory(Map<String,Collection<Integer>> configuration) {
        this.configuration = configuration;
    }

    @Override
    public LoadBalancingOperation move(Integer segmentIdentifier, TrackingEventProcessor trackingEventProcessor,
                                       String sourceInstance, String targetInstance) {
        return () -> {
            assert configuration.get(sourceInstance).contains(segmentIdentifier);
            configuration.get(sourceInstance).remove(segmentIdentifier);
            configuration.get(targetInstance).add(segmentIdentifier);
        };
    }
}