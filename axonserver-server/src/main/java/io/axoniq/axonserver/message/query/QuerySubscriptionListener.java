package io.axoniq.axonserver.message.query;

/**
 * Author: marc
 */
public interface QuerySubscriptionListener {
    void subscribeQuery(QueryDefinition query, String component, String clientName, int nrOfHandlers);
    void unsubscribeQuery(QueryDefinition queryDefinition, String componentName, String client);
}
