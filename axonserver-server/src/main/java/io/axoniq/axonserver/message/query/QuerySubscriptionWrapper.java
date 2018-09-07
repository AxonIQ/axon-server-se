package io.axoniq.axonserver.message.query;

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
