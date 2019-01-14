package io.axoniq.axonserver.message.query;

import java.util.NavigableSet;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Marc Gathier
 */
public class RoundRobinQueryHandlerSelector implements QueryHandlerSelector {
    private final ConcurrentMap<QueryDefinitionComponent, String> lastClientMap = new ConcurrentHashMap<>();

    @Override
    public String select(QueryDefinition queryDefinition, String componentName, NavigableSet<String> queryHandlers) {
        if( queryHandlers.isEmpty()) return null;
        QueryDefinitionComponent key = new QueryDefinitionComponent(queryDefinition, componentName);
        String last = lastClientMap.get(key);
        if( last == null) {
            last = queryHandlers.first();
            lastClientMap.put(key, last);
            return last;
        }

        SortedSet<String> tail = queryHandlers.tailSet(last, false);
        if( tail.isEmpty()) {
            last = queryHandlers.first();
        } else  {
            last = tail.first();
        }
        lastClientMap.put(key, last);
        return last;
    }

    private static class QueryDefinitionComponent {
        private final QueryDefinition queryDefinition;
        private final String componentName;

        private QueryDefinitionComponent(QueryDefinition queryDefinition, String componentName) {
            this.queryDefinition = queryDefinition;
            this.componentName = componentName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryDefinitionComponent that = (QueryDefinitionComponent) o;
            return Objects.equals(queryDefinition, that.queryDefinition) &&
                    Objects.equals(componentName, that.componentName);
        }

        @Override
        public int hashCode() {

            return Objects.hash(queryDefinition, componentName);
        }
    }
}
