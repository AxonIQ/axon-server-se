package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.message.ClientIdentification;

import java.util.NavigableSet;

/**
 * Author: marc
 */
public interface QueryHandlerSelector {
    ClientIdentification select(QueryDefinition queryDefinition, String componentName, NavigableSet<ClientIdentification> queryHandlers);
}
