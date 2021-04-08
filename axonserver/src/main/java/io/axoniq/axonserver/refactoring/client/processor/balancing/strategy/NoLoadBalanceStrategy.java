/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.client.processor.balancing.strategy;

import io.axoniq.axonserver.refactoring.client.processor.balancing.LoadBalancingOperation;
import io.axoniq.axonserver.refactoring.client.processor.balancing.LoadBalancingStrategy;
import io.axoniq.axonserver.refactoring.client.processor.balancing.TrackingEventProcessor;
import org.springframework.stereotype.Component;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
 */
public class NoLoadBalanceStrategy implements LoadBalancingStrategy {

    @Override
    public LoadBalancingOperation balance(TrackingEventProcessor processor) {
        return () -> {
        };
    }

    @Component("NoLoadBalance")
    public static final class Factory implements LoadBalancingStrategy.Factory {


        @Override
        public LoadBalancingStrategy create() {
            return new NoLoadBalanceStrategy();
        }
    }
}
