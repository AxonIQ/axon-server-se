/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

/**
 * Cache to keep track of query handlers registered. It tracks the registered queries for each client and the clients
 * for each query.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Component("QueryRegistrationCache")
public class QueryRegistrationCache {

    private final QueryHandlerSelector queryHandlerSelector;
    private final BiFunction<Map<String, MetaDataValue>, Set<ClientStreamIdentification>, Set<ClientStreamIdentification>> metaDataBasedNodeSelector;

    private final Map<QueryDefinition, QueryInformation> registrationsPerQuery = new ConcurrentHashMap<>();

    /**
     * Autowired constructor.
     *
     * @param queryHandlerSelector      selector to choose one application instance to handle a query when there are
     *                                  multiple instances of the same application
     * @param metaDataBasedNodeSelector selector to filter the candidates for a query based on the meta data in the
     *                                  query
     */
    @Autowired
    public QueryRegistrationCache(QueryHandlerSelector queryHandlerSelector,
                                  BiFunction<Map<String, MetaDataValue>, Set<ClientStreamIdentification>, Set<ClientStreamIdentification>> metaDataBasedNodeSelector) {
        this.queryHandlerSelector = queryHandlerSelector;
        this.metaDataBasedNodeSelector = metaDataBasedNodeSelector;
    }

    /**
     * Constructor that uses a no-op meta data filter.
     *
     * @param queryHandlerSelector selector to choose one application instance to handle a query when there are multiple
     *                             instances of the same application
     */
    public QueryRegistrationCache(QueryHandlerSelector queryHandlerSelector) {
        this(queryHandlerSelector, (metadata, clients) -> clients);
    }

    /**
     * Removes registration for a query handler.
     *
     * @param event the unsubscription event
     */
    @EventListener
    public void on(SubscriptionEvents.UnsubscribeQuery event) {
        QuerySubscription unsubscribe = event.getUnsubscribe();
        QueryDefinition queryDefinition = new QueryDefinition(event.getContext(), unsubscribe.getQuery());
        remove(queryDefinition, event.clientIdentification());
    }

    /**
     * Adds a registration for a query handler.
     *
     * @param event the subscription event
     */
    @EventListener
    public void on(SubscriptionEvents.SubscribeQuery event) {
        QuerySubscription subscription = event.getSubscription();
        QueryDefinition queryDefinition = new QueryDefinition(event.getContext(), subscription.getQuery());
        add(queryDefinition, subscription.getResultName(), event.getQueryHandler());
    }

    @EventListener
    public void on(TopologyEvents.QueryHandlerDisconnected queryHandlerDisconnected) {
        remove(queryHandlerDisconnected.clientIdentification());
    }

    /**
     * Removes all registered queries for a client
     *
     * @param clientStreamIdentification the client identification
     */
    public void remove(ClientStreamIdentification clientStreamIdentification) {
        registrationsPerQuery.forEach((k, v) -> v.removeClient(clientStreamIdentification));
        registrationsPerQuery.entrySet().removeIf(v -> v.getValue().isEmpty());
    }

    public void remove(QueryDefinition queryDefinition, ClientStreamIdentification clientId) {
        QueryInformation queryInformation = registrationsPerQuery.get(queryDefinition);
        if (queryInformation != null) {
            queryInformation.removeClient(clientId);
            if (queryInformation.isEmpty()) {
                registrationsPerQuery.remove(queryDefinition);
            }
        }
    }

    public void add(QueryDefinition queryDefinition, String resultName,
                    QueryHandler queryHandler) {
        registrationsPerQuery.computeIfAbsent(queryDefinition, k -> new QueryInformation())
                             .addResultName(resultName)
                             .addHandler(queryHandler);
    }

    /**
     * Finds query handlers for the given request. Possible handlers may be filtered based on meta data in the request.
     * If there are multiple instances of an application, this returns only one of the instances.
     *
     * @param context the name of the context
     * @param request the query request
     * @return a set of {@link QueryHandler}s.
     */
    public Set<QueryHandler<?>> find(String context, QueryRequest request) {
        QueryDefinition queryDefinition = new QueryDefinition(context, request.getQuery());
        QueryInformation queryInformation = registrationsPerQuery.get(queryDefinition);
        if (queryInformation == null) {
            return Collections.emptySet();
        }

        Set<ClientStreamIdentification> candidates = queryInformation.getHandlersPerComponent()
                                                                     .values()
                                                                     .stream()
                                                                     .flatMap(Collection::stream).collect(toSet());
        Set<ClientStreamIdentification> filteredCandidates = metaDataBasedNodeSelector.apply(request.getMetaDataMap(),
                                                                                             candidates);
        return queryInformation.getHandlersPerComponent().entrySet().stream()
                               .map(entry -> pickOne(queryDefinition,
                                                     entry.getKey(),
                                                     entry.getValue(),
                                                     filteredCandidates))
                               .filter(Objects::nonNull)
                               .collect(toSet());
    }

    /**
     * Finds all query handlers for the given request.
     *
     * @param context the name of the context
     * @param request the query request
     * @return a set of {@link QueryHandler}s.
     */
    public Collection<QueryHandler> findAll(String context, QueryRequest request) {
        QueryDefinition def = new QueryDefinition(context, request.getQuery());
        return (registrationsPerQuery.containsKey(def)) ? registrationsPerQuery.get(def).handlers.values() : emptySet();
    }

    private QueryHandler<?> pickOne(QueryDefinition queryDefinition, String componentName,
                                 NavigableSet<ClientStreamIdentification> queryHandlers,
                                 Set<ClientStreamIdentification> filteredCandidates) {
        if (queryHandlers.isEmpty()) {
            return null;
        }
        ClientStreamIdentification client = queryHandlerSelector.select(queryDefinition,
                                                                        componentName,
                                                                        new TreeSet<>(intersect(queryHandlers,
                                                                                                     filteredCandidates)));
        if (client == null) {
            return null;
        }
        return registrationsPerQuery.get(queryDefinition).getHandler(client);
    }

    private Set<ClientStreamIdentification>  intersect(Set<ClientStreamIdentification> first, Set<ClientStreamIdentification> second) {
        Set<ClientStreamIdentification> result = new HashSet<>(first);
        result.removeIf(item -> !second.contains(item));
        return result;
    }

    public Map<QueryDefinition, Map<String, Set<QueryHandler<?>>>> getAll() {
        Map<QueryDefinition, Map<String, Set<QueryHandler<?>>>> all = new HashMap<>();
        registrationsPerQuery.forEach((query, queryInformation) -> {
            Map<String, Set<QueryHandler<?>>> componentsMap = new HashMap<>();
            all.put(query, componentsMap);
            queryInformation.handlers.values().forEach(h ->
                                                               componentsMap.computeIfAbsent(h.getComponentName(),
                                                                                             c -> new HashSet<>())
                                                                            .add(h)
            );
        });
        return all;
    }

    public List<QueryRegistration> getForClient(ClientStreamIdentification client) {
        return registrationsPerQuery.entrySet().stream()
                                    .map(e -> new QueryRegistration(e.getKey(),
                                                                    e.getValue().getHandler(client)))
                                    .filter(r -> r.queryHandler != null && r.queryHandler
                                            .getClientStreamIdentification().equals(client))
                                    .collect(Collectors.toList());
    }

    public QueryHandler find(String context, QueryRequest request, String clientStreamId) {
        QueryDefinition queryDefinition = new QueryDefinition(context, request.getQuery());
        return find(queryDefinition, clientStreamId);
    }

    public QueryHandler<?> find(QueryDefinition queryDefinition, String clientStreamId) {
        ClientStreamIdentification clientStreamIdentification =
                new ClientStreamIdentification(queryDefinition.getContext(), clientStreamId);
        QueryInformation queryInformation = registrationsPerQuery.get(queryDefinition);
        return queryInformation == null ? null : queryInformation.getHandler(clientStreamIdentification);
    }

    public Set<ClientStreamIdentification> getClients() {
        return registrationsPerQuery.values().stream().flatMap(q -> q.handlers.keySet().stream()).collect(toSet());
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

        private final Map<ClientStreamIdentification, QueryHandler> handlers = new ConcurrentHashMap<>();
        private final Set<String> resultNames = new CopyOnWriteArraySet<>();

        public void removeClient(ClientStreamIdentification clientId) {
            handlers.remove(clientId);
        }

        public boolean isEmpty() {
            return handlers.isEmpty();
        }

        public QueryInformation addResultName(String resultName) {
            resultNames.add(resultName);
            return this;
        }

        public QueryInformation addHandler(QueryHandler queryHandler) {
            handlers.put(queryHandler.getClientStreamIdentification(), queryHandler);
            return this;
        }

        public QueryHandler getHandler(ClientStreamIdentification client) {
            return handlers.get(client);
        }

        public Map<String, NavigableSet<ClientStreamIdentification>> getHandlersPerComponent() {
            Map<String, NavigableSet<ClientStreamIdentification>> map = new HashMap<>();
            handlers.values().forEach(queryHandler -> map
                    .computeIfAbsent(queryHandler.getComponentName(), c -> new TreeSet<>())
                    .add(queryHandler.getClientStreamIdentification()));
            return map;
        }
    }

}
