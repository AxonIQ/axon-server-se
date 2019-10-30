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
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.serializer.Printable;
import org.slf4j.Logger;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.Set;

/**
 * Rest APIs related to the load balancing of tracking event processors over client applications.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
@RestController
@Transactional
@RequestMapping("v1")
public class LoadBalancingRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final LoadBalanceStrategyHolder strategyController;
    private final ProcessorLoadBalanceStrategy processorLoadBalanceStrategy;

    public LoadBalancingRestController(
            LoadBalanceStrategyHolder strategyController,
            ProcessorLoadBalanceStrategy processorLoadBalanceStrategy) {
        this.strategyController = strategyController;
        this.processorLoadBalanceStrategy = processorLoadBalanceStrategy;
    }

    @GetMapping("processors/loadbalance/strategies")
    public Iterable<? extends Printable> getStrategies(final Principal principal) {
        auditLog.debug("[{}] Request to list load-balancing strategies.", AuditLog.username(principal));

        return strategyController.findAll();
    }

    @GetMapping("processors/loadbalance/strategies/factories")
    public Set<String> getLoadBalancingStrategyFactoryBean(final Principal principal){
        auditLog.debug("[{}] Request to list load-balancing strategy factories.", AuditLog.username(principal));

        return strategyController.getFactoryBeans();
    }

    @PatchMapping("components/{component}/processors/{processor}/loadbalance")
    @Deprecated
    public void loadBalance(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @RequestParam("context") String context,
                            @RequestParam("strategy") String strategyName,
                            final Principal principal) {
        auditLog.debug("[{}@{}] Request to set load-balancing strategy for processor \"{}\" in component \"{}\" to \"{}\", using a deprecated API.",
                       AuditLog.username(principal), context,
                       processor, component, strategyName);

        loadBalance(processor, component, strategyName, principal);
    }

    @PatchMapping("processors/{processor}/loadbalance")
    public void loadBalance(@PathVariable("processor") String processor,
                            @RequestParam("context") String context,
                            @RequestParam("strategy") String strategyName,
                            final Principal principal) {
        auditLog.debug("[{}@{}] Request to set load-balancing strategy for processor \"{}\" to \"{}\".",
                       AuditLog.username(principal), context,
                       processor, strategyName);

        TrackingEventProcessor trackingProcessor = new TrackingEventProcessor(processor, context);
        processorLoadBalanceStrategy.balance(trackingProcessor, strategyName).perform();
    }
}
