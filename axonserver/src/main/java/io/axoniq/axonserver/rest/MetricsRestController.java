package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.CommandMetricsRegistry;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@RestController
@RequestMapping("/v1/public")
public class MetricsRestController {

    private final CommandRegistrationCache commandRegistrationCache;
    private final CommandMetricsRegistry commandMetricsRegistry;
    private final QueryRegistrationCache queryRegistrationCache;
    private final QueryMetricsRegistry queryMetricsRegistry;

    public MetricsRestController(CommandRegistrationCache commandRegistrationCache, CommandMetricsRegistry commandMetricsRegistry, QueryRegistrationCache queryRegistrationCache, QueryMetricsRegistry queryMetricsRegistry) {
        this.commandRegistrationCache = commandRegistrationCache;
        this.commandMetricsRegistry = commandMetricsRegistry;
        this.queryRegistrationCache = queryRegistrationCache;
        this.queryMetricsRegistry = queryMetricsRegistry;
    }


    @GetMapping("/command-metrics")
    public List<CommandMetricsRegistry.CommandMetric> getCommandMetrics() {
        List<CommandMetricsRegistry.CommandMetric> metrics = new ArrayList<>();
        commandRegistrationCache.getAll().forEach((commandHander, registrations) -> metrics.addAll(getMetrics(commandHander, registrations)));
        return metrics;
    }

    private List<CommandMetricsRegistry.CommandMetric> getMetrics(CommandHandler commandHander, Set<CommandRegistrationCache.RegistrationEntry> registrations) {
        return registrations.stream()
                .map(registration -> commandMetricsRegistry.commandMetric(registration.getCommand(), commandHander.getClient(), commandHander.getComponentName()))
               // .filter(m -> m.getCount() == 0)
                .collect(Collectors.toList());
    }

    @GetMapping("/query-metrics")
    public List<QueryMetricsRegistry.QueryMetric> getQueryMetrics() {
        List<QueryMetricsRegistry.QueryMetric> metrics = new ArrayList<>();
        queryRegistrationCache.getAll().forEach((queryDefinition, handlersPerComponent) -> metrics.addAll(getQueryMetrics(queryDefinition, handlersPerComponent)));
        return metrics;
    }

    private List<QueryMetricsRegistry.QueryMetric> getQueryMetrics(QueryDefinition queryDefinition, Map<String, Set<QueryHandler>> handlersPerComponent) {
        return handlersPerComponent.entrySet().stream()
                .map(queryHandlers -> queryHandlers.getValue().stream().map(queryHandler ->
                        queryMetricsRegistry.queryMetric(queryDefinition, queryHandler.getClientName(), queryHandlers.getKey())
                ).collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
