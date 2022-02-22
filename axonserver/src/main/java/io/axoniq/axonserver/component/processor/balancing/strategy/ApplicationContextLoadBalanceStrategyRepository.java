/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marc Gathier
 * @author Allard Buijze
 */
@Component
public class ApplicationContextLoadBalanceStrategyRepository implements LoadBalanceStrategyRepository {

    private final Map<String, LoadBalancingStrategy> strategies = new ConcurrentHashMap<>();

    public ApplicationContextLoadBalanceStrategyRepository(List<LoadBalancingStrategy> loadBalancingFactories) {
        strategies.put("default", NoLoadBalanceStrategy.INSTANCE);
        loadBalancingFactories.forEach(s -> strategies.put(s.getName(), s));
    }

    @Override
    public LoadBalancingStrategy findByName(String strategyName) {
        return strategies.getOrDefault(strategyName, NoLoadBalanceStrategy.INSTANCE);
    }

    @Override
    public Iterable<LoadBalancingStrategy> findAll() {
        return strategies.values();
    }

}
