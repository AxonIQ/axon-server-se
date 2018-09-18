package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.query.DefaultQueries;
import io.axoniq.axonserver.component.query.Query;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.platform.KeepNames;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@RestController("QueryRestController")
@RequestMapping("/v1")
public class QueryRestController {

    private final QueryRegistrationCache registrationCache;


    public QueryRestController(QueryRegistrationCache registrationCache) {
        this.registrationCache = registrationCache;
    }

    @GetMapping("components/{component}/queries")
    public Iterable<Query> getByComponent(@PathVariable("component") String component, @RequestParam("context") String context){
        return new ComponentItems<>(component, context, new DefaultQueries(registrationCache));
    }

    @GetMapping("queries")
    public List<JsonQueryMapping> get() {
        return registrationCache.getAll().entrySet().stream().map(e-> JsonQueryMapping.from(e, registrationCache.getResponseTypes(e.getKey()))).collect(Collectors.toList());
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
