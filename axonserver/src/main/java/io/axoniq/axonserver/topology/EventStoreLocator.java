package io.axoniq.axonserver.topology;

import io.axoniq.axonserver.message.event.EventStore;

/**
 * Author: marc
 */
public interface EventStoreLocator {

    boolean isMaster(String nodeName, String contextName);

    EventStore getEventStore(String context);

    String getMaster(String context);
}
