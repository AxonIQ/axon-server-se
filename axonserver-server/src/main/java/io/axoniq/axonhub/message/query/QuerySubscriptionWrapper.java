package io.axoniq.axonhub.message.query;

/**
 * Author: marc
 */
public interface QuerySubscriptionWrapper {
    String getQuery();
    String getResultName();
    String getComponentName();
    String getClient();
    int getNrOfHandlers();
}
