package io.axoniq.axonserver.message.query;

import java.util.NavigableSet;

/**
 * @author Marc Gathier
 */
public interface QueryHandlerSelector {
    String select(QueryDefinition queryDefinition, String componentName, NavigableSet<String> queryHandlers);
}
