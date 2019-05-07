package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

/**
 * @author Marc Gathier
 */
public interface SyncStrategy {
    boolean storeOnNode(EventTypeContext eventTypeContext);
}
