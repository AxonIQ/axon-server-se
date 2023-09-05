/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonserver.message.query.subscription.metric.SubscriptionQueryMetricRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Rest Service to retrieve metrics (counters) on subscription queries.
 * @author Sara Pellegrini
 * @since 4.0
 */
@RestController
@RequestMapping("/v1/components/{componentName}/subscription-query-metric")
public class SubscriptionQueryMetricRestController {

    private final SubscriptionQueryMetricRegistry applicationRegistry;


    public SubscriptionQueryMetricRestController(
            SubscriptionQueryMetricRegistry applicationRegistry) {
        this.applicationRegistry = applicationRegistry;
    }

    @GetMapping
    public SubscriptionMetrics getForComponent(@PathVariable("componentName") String componentName,
                                               @RequestParam("context") String context) {
        return applicationRegistry.getByComponentAndContext(componentName, context);
    }

    @GetMapping("query/{queryName}")
    public SubscriptionMetrics getForComponentAndQuery(@PathVariable("componentName") String componentName,
                                                       @PathVariable("queryName") String queryName,
                                                       @RequestParam("context") String context) {
        return applicationRegistry.getByComponentAndQuery(componentName, queryName, context);
    }
}
