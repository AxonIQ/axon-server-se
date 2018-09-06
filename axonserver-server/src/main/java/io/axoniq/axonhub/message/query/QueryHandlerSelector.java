package io.axoniq.axonhub.message.query;

import java.util.NavigableSet;

/**
 * Author: marc
 */
public interface QueryHandlerSelector {
    String select(QueryDefinition queryDefinition, String componentName, NavigableSet<String> queryHandlers);
}
