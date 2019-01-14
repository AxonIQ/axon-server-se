package io.axoniq.axonserver.topology;

import io.axoniq.axonserver.message.event.EventStore;

/**
 * @author Marc Gathier
 */
public interface EventStoreLocator {

    boolean isMaster(String nodeName, String contextName);

    EventStore getEventStore(String context);

    String getMaster(String context);
}
