/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.serializer.Printable;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Repository
public class SimpleLoadBalanceStrategyHolder implements LoadBalanceStrategyHolder {
    private final Map<String, LoadBalancingStrategy> loadBalancingStrategyMap = new ConcurrentHashMap<>();

    public SimpleLoadBalanceStrategyHolder() {
        LoadBalancingStrategy noLoadBalanceStrategy = new LoadBalancingStrategy("default", "Default (disabled)", "NoLoadBalance");
        loadBalancingStrategyMap.put(noLoadBalanceStrategy.name(), noLoadBalanceStrategy);

        LoadBalancingStrategy threadNumber = new LoadBalancingStrategy("threadNumber", "Thread Number", "ThreadNumberBalancingStrategy");
        loadBalancingStrategyMap.put(threadNumber.name(), threadNumber);
    }

    @Override
    public LoadBalancingStrategy findByName(String strategyName) {
        return loadBalancingStrategyMap.get(strategyName);
    }

    @Override
    public Iterable<? extends Printable> findAll() {
        return loadBalancingStrategyMap.values();
    }

    @Override
    public Set<String> getFactoryBeans() {
        return loadBalancingStrategyMap.values().stream()
                                       .map(LoadBalancingStrategy::factoryBean)
                                       .collect(Collectors.toSet());
    }
}
