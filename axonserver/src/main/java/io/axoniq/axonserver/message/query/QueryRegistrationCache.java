package io.axoniq.axonserver.message.query;

import io.axoniq.axonhub.QueryRequest;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;

/**
 * Author: marc
 */
@Component("QueryRegistrationCache")
public class QueryRegistrationCache {
    private final QueryHandlerSelector queryHandlerSelector;

    private final Map<QueryDefinition, QueryInformation> registrationsPerQuery = new ConcurrentHashMap<>();

    public QueryRegistrationCache(QueryHandlerSelector queryHandlerSelector) {
        this.queryHandlerSelector = queryHandlerSelector;
    }

    public void remove(String clientId) {
        registrationsPerQuery.forEach((k, v) -> v.removeClient(clientId));
        registrationsPerQuery.entrySet().removeIf(v -> v.getValue().isEmpty());
    }

    public void remove(QueryDefinition queryDefinition, String clientId) {
        QueryInformation queryInformation = registrationsPerQuery.get(queryDefinition);
        if( queryInformation != null) {
            queryInformation.removeClient(clientId);
            if( queryInformation.isEmpty()) registrationsPerQuery.remove(queryDefinition);
        }

    }

    public void add(QueryDefinition queryDefinition, String resultName,
                    QueryHandler queryHandler) {
        registrationsPerQuery.computeIfAbsent(queryDefinition, k -> new QueryInformation())
                             .addResultName(resultName)
                             .addHandler(queryHandler);
    }

    public Set<QueryHandler> find(String context, QueryRequest request) {
        QueryDefinition queryDefinition = new QueryDefinition(context, request.getQuery());
        QueryInformation queryInformation = registrationsPerQuery.get(queryDefinition);
        if( queryInformation == null) return Collections.emptySet();

        return queryInformation.getHandlersPerComponent().entrySet().stream()
                                                    .map( entry -> pickOne(queryDefinition, entry.getKey(), entry.getValue()))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toSet());
    }

    public Set<QueryHandler> findAll(String context, QueryRequest request) {
        QueryDefinition def = new QueryDefinition(context, request.getQuery());
        return (registrationsPerQuery.containsKey(def)) ?  registrationsPerQuery.get(def).handlers : emptySet();
    }

    private QueryHandler pickOne(QueryDefinition queryDefinition, String componentName, NavigableSet<String> queryHandlers) {
        if (queryHandlers.isEmpty()) return null;
        String client = queryHandlerSelector.select(queryDefinition, componentName, queryHandlers);
        if( client == null) return null;
        return registrationsPerQuery.get(queryDefinition).getHandler(client);
    }

    public Map<QueryDefinition, Map<String, Set<QueryHandler>>> getAll() {
        Map<QueryDefinition, Map<String, Set<QueryHandler>>> all = new HashMap<>();
        registrationsPerQuery.forEach((query,queryInformation) -> {
            Map<String, Set<QueryHandler>> componentsMap = new HashMap<>();
            all.put(query, componentsMap);
            queryInformation.handlers.forEach(h ->
                componentsMap.computeIfAbsent(h.getComponentName(), c -> new HashSet<>()).add(h)
            );
        });
        return all;
    }

    public List<QueryRegistration> getForClient(String client) {
        return registrationsPerQuery.entrySet().stream()
                                    .map(e -> new QueryRegistration(e.getKey(),
                                                                    e.getValue().getHandler(client)))
                                    .filter(r -> r.queryHandler != null && r.queryHandler.getClientName().equals(client))
                                    .collect(Collectors.toList());
    }

    public QueryHandler find(String context, QueryRequest request, String client) {
        QueryDefinition queryDefinition = new QueryDefinition(context, request.getQuery());
        return registrationsPerQuery.get(queryDefinition).getHandler(client);
    }

    public Set<String> getClients() {
        return registrationsPerQuery.values().stream().flatMap(q -> q.handlers.stream().map(QueryHandler::getClientName)).distinct().collect(Collectors.toSet());
    }

    public Set<String> getResponseTypes(QueryDefinition key) {
        return registrationsPerQuery.get(key).resultNames;
    }

    public static class QueryRegistration {
        final QueryDefinition queryDefinition;
        final QueryHandler queryHandler;

        public QueryRegistration(QueryDefinition queryDefinition, QueryHandler queryHandler) {
            this.queryDefinition = queryDefinition;
            this.queryHandler = queryHandler;
        }

        public QueryDefinition getQueryDefinition() {
            return queryDefinition;
        }

        public QueryHandler getQueryHandler() {
            return queryHandler;
        }
    }

    private class QueryInformation {
        private final Set<QueryHandler> handlers = new CopyOnWriteArraySet<>();
        private final Set<String> resultNames = new CopyOnWriteArraySet<>();
        public void removeClient(String clientId) {
            handlers.removeIf(h -> h.getClientName().equals(clientId));
        }

        public boolean isEmpty() {
            return handlers.isEmpty();
        }

        public QueryInformation addResultName(String resultName) {
            resultNames.add(resultName);
            return this;
        }

        public QueryInformation addHandler(QueryHandler queryHandler) {
            handlers.add(queryHandler);
            return this;
        }

        public QueryHandler getHandler(String client) {
            return handlers.stream().filter(h -> h.getClientName().equals(client)).findFirst().orElse(null);
        }

        public Map<String, NavigableSet<String>> getHandlersPerComponent() {
            Map<String,NavigableSet<String>> map = new HashMap<>();
            handlers.forEach(queryHandler -> map.computeIfAbsent(queryHandler.getComponentName(), c -> new TreeSet<>()).add(queryHandler.getClientName()));
            return map;
        }
    }

}
