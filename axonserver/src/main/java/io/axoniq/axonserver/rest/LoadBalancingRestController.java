/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.strategy.LoadBalanceStrategyHolder;
import io.axoniq.axonserver.component.processor.balancing.strategy.ProcessorLoadBalanceStrategy;
import io.axoniq.axonserver.serializer.Printable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
@RestController
@Transactional
@RequestMapping("v1")
public class LoadBalancingRestController {

    private final LoadBalanceStrategyHolder strategyController;
    private final ProcessorLoadBalanceStrategy processorLoadBalanceStrategy;

    public LoadBalancingRestController(
            LoadBalanceStrategyHolder strategyController,
            ProcessorLoadBalanceStrategy processorLoadBalanceStrategy) {
        this.strategyController = strategyController;
        this.processorLoadBalanceStrategy = processorLoadBalanceStrategy;
    }

    @GetMapping("processors/loadbalance/strategies")
    public Iterable<? extends Printable> getStrategies() {
        return strategyController.findAll();
    }

    @GetMapping("processors/loadbalance/strategies/factories")
    public Set<String> getLoadBalancingStrategyFactoryBean(){
        return strategyController.getFactoryBeans();
    }

    @PatchMapping("components/{component}/processors/{processor}/loadbalance")
    public void loadBalance(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @RequestParam("context") String context,
                            @RequestParam("strategy") String strategyName) {
        TrackingEventProcessor trackingProcessor = new TrackingEventProcessor(processor, component, context);
        processorLoadBalanceStrategy.balance(trackingProcessor, strategyName).perform();
    }
}
