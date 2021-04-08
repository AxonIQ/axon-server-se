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
import io.axoniq.axonserver.refactoring.client.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.refactoring.client.processor.balancing.jpa.LoadBalancingStrategy;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ProcessorLoadBalanceStrategy {


    private final LoadBalanceStrategyHolder strategyRepository;

    private final Map<String, io.axoniq.axonserver.refactoring.client.processor.balancing.LoadBalancingStrategy.Factory> factories;


    public ProcessorLoadBalanceStrategy(
            LoadBalanceStrategyHolder strategyRepository,
            Map<String, io.axoniq.axonserver.refactoring.client.processor.balancing.LoadBalancingStrategy.Factory> factories) {
        this.strategyRepository = strategyRepository;
        this.factories = factories;
    }

    public LoadBalancingOperation balance(TrackingEventProcessor processor, String strategyName) {
//        String strategyName = repository.findById(processor)
//                                        .map(ProcessorLoadBalancing::strategy)
//                                        .orElse("default");
        LoadBalancingStrategy strategy = strategyRepository.findByName(strategyName);
        io.axoniq.axonserver.refactoring.client.processor.balancing.LoadBalancingStrategy.Factory factory = factories
                .getOrDefault(strategy.factoryBean(), NoLoadBalanceStrategy::new);
        return factory.create().balance(processor);
    }
}
