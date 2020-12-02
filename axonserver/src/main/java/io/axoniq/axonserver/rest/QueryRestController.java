/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.query.DefaultQueries;
import io.axoniq.axonserver.component.query.Query;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.rest.json.QueryRequestJson;
import io.axoniq.axonserver.rest.json.QueryResponseJson;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.ObjectUtils;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import springfox.documentation.annotations.ApiIgnore;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.Valid;

import static io.axoniq.axonserver.AxonServerAccessController.CONTEXT_PARAM;
import static io.axoniq.axonserver.AxonServerAccessController.TOKEN_PARAM;

/**
 * Rest service to retrieve information on registered queries. Also provides service to send a query to query handlers.
 * @author Marc Gathier
 * @since 4.0
 */
@RestController("QueryRestController")
@RequestMapping("/v1")
public class QueryRestController {
    private final Logger logger = LoggerFactory.getLogger(QueryRestController.class);

    private final QueryRegistrationCache registrationCache;
    private final QueryDispatcher queryDispatcher;


    public QueryRestController(QueryRegistrationCache registrationCache,
                               QueryDispatcher queryDispatcher) {
        this.registrationCache = registrationCache;
        this.queryDispatcher = queryDispatcher;
    }

    @GetMapping("components/{component}/queries")
    public Iterable<Query> getByComponent(@PathVariable("component") String component, @RequestParam("context") String context){
        return new ComponentItems<>(component, context, new DefaultQueries(registrationCache));
    }

    @GetMapping("queries")
    public List<JsonQueryMapping> get() {
        return registrationCache.getAll()
                                .entrySet()
                                .stream()
                                .map(e -> JsonQueryMapping.from(e, registrationCache.getResponseTypes(e.getKey())))
                                .collect(Collectors.toList());
    }

    @PostMapping("queries/run")
    @ApiImplicitParams({
            @ApiImplicitParam(name = TOKEN_PARAM, value = "Access Token",
                    required = false, dataType = "string", paramType = "header")
    })
    public SseEmitter execute(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context,
            @RequestBody @Valid QueryRequestJson query,
            @ApiIgnore Authentication principal) {
        SseEmitter sseEmitter = new SseEmitter();
        queryDispatcher.query(new SerializedQuery(context, query.asQueryRequest()),
                              ObjectUtils.getOrDefault(principal, GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL),
                              r -> {
                                  try {
                                      sseEmitter.send(SseEmitter.event().data(new QueryResponseJson(r)));
                                  } catch (IOException e) {
                                      logger.debug("Error while emitting query response", e);
                                  }
                              },
                              completed -> sseEmitter.complete());
        return sseEmitter;
    }


    public static class JsonQueryMapping {
        private String query;
        private Set<String> resultNames;
        private List<JsonComponentMapping> components;

        public String getQuery() {
            return query;
        }

        public Set<String> getResultNames() {
            return resultNames;
        }

        public List<JsonComponentMapping> getComponents() {
            return components;
        }

        public static JsonQueryMapping from(
                Map.Entry<QueryDefinition, Map<String, Set<QueryHandler<?>>>> queryDefinitionEntry,
                Set<String> resultNames) {
            JsonQueryMapping queryMapping = new JsonQueryMapping();
            queryMapping.query = queryDefinitionEntry.getKey().getQueryName();
            queryMapping.resultNames = resultNames;
            queryMapping.components = queryDefinitionEntry.getValue().entrySet().stream()
                                                          .map(JsonComponentMapping::from).collect(Collectors.toList());

            return queryMapping;
        }

    }

    public static class JsonComponentMapping {

        private String component;
        private List<String> clients;

        public String getComponent() {
            return component;
        }

        public List<String> getClients() {
            return clients;
        }

        public static JsonComponentMapping from(Map.Entry<String, Set<QueryHandler<?>>> applicationEntry) {
            JsonComponentMapping jsonApplicationMapping = new JsonComponentMapping();
            jsonApplicationMapping.component = applicationEntry.getKey();
            jsonApplicationMapping.clients = applicationEntry.getValue().stream().map(QueryHandler::toString).collect(
                    Collectors.toList());
            return jsonApplicationMapping;
        }
    }

}
