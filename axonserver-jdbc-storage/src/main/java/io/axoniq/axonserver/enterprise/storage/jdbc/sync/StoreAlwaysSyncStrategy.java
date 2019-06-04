package io.axoniq.axonserver.enterprise.storage.jdbc.sync;

import io.axoniq.axonserver.enterprise.storage.jdbc.SyncStrategy;

/**
 * Implementation of SyncStrategy that causes the events to always be stored (independent of whether the node is leader or not).
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class StoreAlwaysSyncStrategy implements SyncStrategy {

    @Override
    public boolean storeOnNode() {
        return true;
    }
}
