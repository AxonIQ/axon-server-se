/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest;

import io.axoniq.axonserver.admin.eventprocessor.api.LoadBalanceStrategyType;
import io.axoniq.axonserver.admin.eventprocessor.requestprocessor.LocalEventProcessorsAdminService;
import io.axoniq.axonserver.serializer.Printable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import springfox.documentation.annotations.ApiIgnore;

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

    private final LocalEventProcessorsAdminService eventProcessorAdminService;

    public LoadBalancingRestController(LocalEventProcessorsAdminService eventProcessorAdminService) {
        this.eventProcessorAdminService = eventProcessorAdminService;
    }

    @GetMapping("processors/loadbalance/strategies")
    public Iterable<? extends Printable> getStrategies(@ApiIgnore final Principal principal) {
        return eventProcessorAdminService.getBalancingStrategies(principal);
    }

    @Deprecated
    @GetMapping("processors/loadbalance/strategies/factories")
    public Set<String> getLoadBalancingStrategyFactoryBean(@ApiIgnore final Principal principal) {
        return eventProcessorAdminService.getLoadBalancingStrategyFactoryBeans(principal);
    }

    /**
     * Balance the load for the specified event processor among the connected client.
     *
     * @param processor            the event processor name
     * @param tokenStoreIdentifier the token store identifier of the event processor
     * @param strategyName         the strategy to be used to balance the load
     */
    @PatchMapping("processors/{processor}/loadbalance")
    public Mono<Void> balanceProcessorLoad(@PathVariable("processor") String processor,
                                           @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                                           @RequestParam("strategy") String strategyName,
                                           @ApiIgnore final Principal principal) {
        return eventProcessorAdminService.loadBalance(processor, tokenStoreIdentifier,
                LoadBalanceStrategyType.Companion.forStrategy(strategyName),
                new PrincipalAuthentication(principal));
    }
}
