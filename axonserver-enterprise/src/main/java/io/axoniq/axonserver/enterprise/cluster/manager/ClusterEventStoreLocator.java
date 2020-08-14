package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.replication.ContextLeaderProvider;
import io.axoniq.axonserver.enterprise.replication.RaftGroupRepositoryManager;
import io.axoniq.axonserver.enterprise.storage.multitier.MultiTierEventStore;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.EventStore;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.util.ContextNotFoundException;
import org.springframework.stereotype.Controller;

import javax.annotation.Nullable;

/**
 * Finds an event store facade in an Axon Server cluster.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Controller
public class ClusterEventStoreLocator implements EventStoreLocator {

    private final RemoteEventStoreFactory remoteEventStoreFactory;
    private final RaftGroupRepositoryManager raftGroupRepositoryManager;
    private final ContextLeaderProvider leaderProvider;
    private final String nodeName;
    private final MultiTierEventStore multiTierEventStore;
    private final LocalEventStore localEventStore;

    /**
     * @param messagingPlatformConfiguration provides the current node name
     * @param raftGroupRepositoryManager     provides context configuration
     * @param leaderProvider                 provides the leader for a context
     * @param remoteEventStoreFactory        creates a facade for an event store on another node
     * @param multiTierEventStore            handler for event store requests when context uses multi tier storage
     * @param localEventStore                local handler for event store requests
     */
    public ClusterEventStoreLocator(
            MessagingPlatformConfiguration messagingPlatformConfiguration,
            RaftGroupRepositoryManager raftGroupRepositoryManager,
            ContextLeaderProvider leaderProvider,
            RemoteEventStoreFactory remoteEventStoreFactory,
            MultiTierEventStore multiTierEventStore,
            LocalEventStore localEventStore) {
        this.remoteEventStoreFactory = remoteEventStoreFactory;
        this.leaderProvider = leaderProvider;
        this.multiTierEventStore = multiTierEventStore;
        this.raftGroupRepositoryManager = raftGroupRepositoryManager;
        this.localEventStore = localEventStore;
        this.nodeName = messagingPlatformConfiguration.getName();
    }


    /**
     * Finds the event store facade to send event store requests to. Returns null if no valid facade can be found.
     *
     * @param context the context to get the {@link EventStore} for
     * @return the event store facade
     */
    @Override
    public EventStore getEventStore(String context) {
        if (raftGroupRepositoryManager.hasLowerTier(context)) {
            return multiTierEventStore;
        }
        return leaderEventStore(context);
    }

    /**
     * Finds the event store facade to send event store requests to. If {@code useLocal} is true it will check if
     * if can serve the requests from the current node.
     * Returns null if no valid facade can be found.
     *
     * @param context     the context to get the {@link EventStore} for
     * @param forceLeader force reading from leader
     * @return the event store facade
     */
    @Override
    public EventStore getEventStore(String context, boolean forceLeader) {
        try {
            if (raftGroupRepositoryManager.hasLowerTier(context)) {
                return multiTierEventStore;
            }

            if (!forceLeader && raftGroupRepositoryManager.containsStorageContext(context)) {
                return localEventStore;
            }
        } catch (ContextNotFoundException contextNotFoundException) {
            // No information on the specified context found to determine if it is a multi-tier context
            // request is sent to the leader
        }

        return leaderEventStore(context);
    }

    @Nullable
    private EventStore leaderEventStore(String context) {
        String leader = leaderProvider.getLeaderOrWait(context, true);
        if (nodeName.equals(leader)) {
            return localEventStore;
        }

        if (leader == null) {
            return null;
        }
        return remoteEventStoreFactory.create(leader);
    }
}
