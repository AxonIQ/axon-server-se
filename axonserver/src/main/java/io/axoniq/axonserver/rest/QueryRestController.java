package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.DispatchEvents;
import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.query.DefaultQueries;
import io.axoniq.axonserver.component.query.Query;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.platform.KeepNames;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.Valid;

/**
 * Author: marc
 */
@RestController("QueryRestController")
@RequestMapping("/v1")
public class QueryRestController {

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
        return registrationCache.getAll().entrySet().stream().map(e-> JsonQueryMapping.from(e, registrationCache.getResponseTypes(e.getKey()))).collect(Collectors.toList());
    }

    @PostMapping("queries")
    public SseEmitter execute(@RequestBody @Valid QueryRequestJson query) {
        SseEmitter sseEmitter = new SseEmitter();
        DispatchEvents.DispatchQuery dispachQuery = new DispatchEvents.DispatchQuery(Topology.DEFAULT_CONTEXT,
                                                                                     query.asQueryRequest(),
                                                                                     r -> {
                                                                                         try {
                                                                                             sseEmitter.send(SseEmitter.event().data(new QueryResponseJson(r)));
                                                                                         } catch (IOException e) {
                                                                                             e.printStackTrace();
                                                                                         }
                                                                                     },
                                                                                     completed->sseEmitter.complete(),
                                                                                     false);
        queryDispatcher.on(dispachQuery);
        return sseEmitter;
    }


    @KeepNames
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

        public static JsonQueryMapping from(Map.Entry<QueryDefinition, Map<String, Set<QueryHandler>>> queryDefinitionEntry, Set<String> resultNames) {
            JsonQueryMapping queryMapping = new JsonQueryMapping();
            queryMapping.query = queryDefinitionEntry.getKey().getQueryName();
            queryMapping.resultNames = resultNames;
            queryMapping.components = queryDefinitionEntry.getValue().entrySet().stream().map(JsonComponentMapping::from).collect(Collectors.toList());

            return queryMapping;
        }

    }

    @KeepNames
    public static class JsonComponentMapping {

        private String component;
        private List<String> clients;

        public String getComponent() {
            return component;
        }

        public List<String> getClients() {
            return clients;
        }

        public static JsonComponentMapping from(Map.Entry<String, Set<QueryHandler>> applicationEntry) {
            JsonComponentMapping jsonApplicationMapping = new JsonComponentMapping();
            jsonApplicationMapping.component = applicationEntry.getKey();
            jsonApplicationMapping.clients = applicationEntry.getValue().stream().map(QueryHandler::toString).collect(Collectors.toList());
            return jsonApplicationMapping;
        }
    }

}
