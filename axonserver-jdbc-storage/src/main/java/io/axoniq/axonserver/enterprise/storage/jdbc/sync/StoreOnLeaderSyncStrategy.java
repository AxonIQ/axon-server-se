package io.axoniq.axonserver.enterprise.storage.jdbc.sync;

import io.axoniq.axonserver.enterprise.storage.jdbc.SyncStrategy;

import java.util.function.Predicate;

/**
 *  * Implementation of SyncStrategy that causes the events to be stored only if the node is currently leader.
 * @author Marc Gathier
 */
public class StoreOnLeaderSyncStrategy implements SyncStrategy {

    private final String context;
    private final Predicate<String>  raftLeaderProvider;

    public StoreOnLeaderSyncStrategy(String context, Predicate<String> raftLeaderProvider) {
        this.context = context;
        this.raftLeaderProvider = raftLeaderProvider;
    }

    @Override
    public boolean storeOnNode() {
        return raftLeaderProvider.test(context);
    }
}
