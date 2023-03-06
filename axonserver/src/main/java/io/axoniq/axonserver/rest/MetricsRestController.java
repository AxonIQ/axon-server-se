/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.CommandMetricsRegistry;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.transport.rest.PrincipalAuthentication;
import io.swagger.v3.oas.annotations.Parameter;
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rest service to retrieve metrics on commands and queries. Returns the counters of commands/queries per handling
 * application.
 *
 * @author Marc Gathier
 */
@RestController
@RequestMapping("/v1/public")
public class MetricsRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final CommandRegistrationCache commandRegistrationCache;
    private final CommandMetricsRegistry commandMetricsRegistry;
    private final QueryRegistrationCache queryRegistrationCache;
    private final QueryMetricsRegistry queryMetricsRegistry;
    private final Topology topology;

    public MetricsRestController(CommandRegistrationCache commandRegistrationCache,
                                 CommandMetricsRegistry commandMetricsRegistry,
                                 QueryRegistrationCache queryRegistrationCache,
                                 QueryMetricsRegistry queryMetricsRegistry,
                                 Topology topology) {
        this.commandRegistrationCache = commandRegistrationCache;
        this.commandMetricsRegistry = commandMetricsRegistry;
        this.queryRegistrationCache = queryRegistrationCache;
        this.queryMetricsRegistry = queryMetricsRegistry;
        this.topology = topology;
    }


    @GetMapping("/command-metrics")
    public List<CommandMetricsRegistry.CommandMetric> getCommandMetrics(
            @Parameter(hidden = true) final Principal principal) {
        auditLog.debug("[{}] Request to list command metrics.", AuditLog.username(principal));

        List<CommandMetricsRegistry.CommandMetric> metrics = new ArrayList<>();
        Set<String> contexts = topology.visibleContexts(false, new PrincipalAuthentication(principal));
        commandRegistrationCache.getAll()
                                .forEach((commandHander, registrations) ->
                                                 metrics.addAll(getMetrics(commandHander, contexts, registrations)));
        return metrics;
    }

    private List<CommandMetricsRegistry.CommandMetric> getMetrics(CommandHandler commandHander,
                                                                  Set<String> contexts,
                                                                  Set<CommandRegistrationCache.RegistrationEntry> registrations) {
        return registrations.stream()
                .filter(registration -> contexts.contains(registration.getContext()))
                            .map(registration -> commandMetricsRegistry
                                    .commandMetric(registration.getCommand(),
                                                   commandHander.getClientId(),
                                                   commandHander.getClientStreamIdentification().getContext(),
                                                   commandHander.getComponentName()))
                            // .filter(m -> m.getCount() == 0)
                            .collect(Collectors.toList());
    }

    @GetMapping("/query-metrics")
    public List<QueryMetricsRegistry.QueryMetric> getQueryMetrics(@Parameter(hidden = true) final Principal principal) {
        auditLog.debug("[{}] Request to list query metrics.", AuditLog.username(principal));
        Set<String> contexts = topology.visibleContexts(false, new PrincipalAuthentication(principal));
        List<QueryMetricsRegistry.QueryMetric> metrics = new ArrayList<>();
        queryRegistrationCache.getAll().forEach((queryDefinition, handlersPerComponent) -> metrics
                .addAll(getQueryMetrics(queryDefinition, contexts, handlersPerComponent)));
        return metrics;
    }

    private List<QueryMetricsRegistry.QueryMetric> getQueryMetrics(QueryDefinition queryDefinition,
                                                                   Set<String> contexts,
                                                                   Map<String, Set<QueryHandler<?>>> handlersPerComponent) {
        return handlersPerComponent.entrySet()
                                   .stream()
                                   .map(queryHandlers -> queryHandlers
                                           .getValue()
                                           .stream()
                                           .filter(queryHandler -> contexts.contains(queryHandler.getClientStreamIdentification().getContext()))
                                           .map(queryHandler -> queryMetricsRegistry
                                                   .queryMetric(
                                                           queryDefinition,
                                                           queryHandler.getClientId(),
                                                           queryHandler.getClientStreamIdentification().getContext(),
                                                           queryHandlers.getKey())
                                           ).collect(Collectors.toList()))
                                   .flatMap(Collection::stream)
                                   .collect(Collectors.toList());
    }
}
