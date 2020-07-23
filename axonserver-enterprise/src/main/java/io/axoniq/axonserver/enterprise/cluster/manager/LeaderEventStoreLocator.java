package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.replication.ContextLeaderProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.EventStore;
import org.springframework.stereotype.Controller;

/**
 * Component that returns an event store object to send requests to the leader of a context.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Controller
public class LeaderEventStoreLocator {

    private final ContextLeaderProvider raftLeaderProvider;
    private final String nodeName;
    private final RemoteEventStoreFactory remoteEventStoreFactory;
    private final LocalEventStore localEventStore;

    public LeaderEventStoreLocator(
            MessagingPlatformConfiguration messagingPlatformConfiguration,
            ContextLeaderProvider raftLeaderProvider,
            LocalEventStore localEventStore,
            RemoteEventStoreFactory remoteEventStoreFactory) {
        this.raftLeaderProvider = raftLeaderProvider;
        this.localEventStore = localEventStore;
        this.remoteEventStoreFactory = remoteEventStoreFactory;
        this.nodeName = messagingPlatformConfiguration.getName();
    }


    /**
     * Finds the {@link EventStore} facade for the leader of the context. Throws {@link MessagingPlatformException}
     * when there currently not is a leader for the context.
     *
     * @param context the context to find the leader for
     * @return the EventStore facade for the leader
     */
    public EventStore getEventStore(String context) {
        String leader = raftLeaderProvider.getLeaderOrWait(context, true);
        if (nodeName.equals(leader)) {
            return localEventStore;
        }
        if (leader == null) {
            throw new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE, context + ": No leader found");
        }
        return remoteEventStoreFactory.create(leader);
    }
}
