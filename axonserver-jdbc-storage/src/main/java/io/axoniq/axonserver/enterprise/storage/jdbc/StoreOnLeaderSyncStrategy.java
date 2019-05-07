package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

import java.util.function.Predicate;

/**
 * @author Marc Gathier
 */
public class StoreOnLeaderSyncStrategy implements SyncStrategy {

    private final Predicate<String>  raftLeaderProvider;

    public StoreOnLeaderSyncStrategy(Predicate<String> raftLeaderProvider) {
        this.raftLeaderProvider = raftLeaderProvider;
    }

    @Override
    public boolean storeOnNode(EventTypeContext eventTypeContext) {
        return raftLeaderProvider.test(eventTypeContext.getContext());
    }
}
