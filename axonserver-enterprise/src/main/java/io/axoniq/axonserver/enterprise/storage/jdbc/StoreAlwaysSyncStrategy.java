package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

/**
 * @author Marc Gathier
 */
public class StoreAlwaysSyncStrategy implements SyncStrategy {

    @Override
    public boolean storeOnNode(EventTypeContext eventTypeContext) {
        return true;
    }
}
