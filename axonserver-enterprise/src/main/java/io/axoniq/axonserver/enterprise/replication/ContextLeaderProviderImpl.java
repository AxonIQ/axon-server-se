package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Component that provides information on the leader for a raft group containing contexts.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class ContextLeaderProviderImpl implements ContextLeaderProvider {

    private final Map<String, String> leaderMap = new ConcurrentHashMap<>();

    private final String node;
    private final int waitForLeaderTimeout;

    /**
     * Constructor autowired by application
     * @param configuration configuration of the Axon Server node
     * @param raftProperties raft properties
     */
    @Autowired
    public ContextLeaderProviderImpl(MessagingPlatformConfiguration configuration,
                                     RaftProperties raftProperties) {
        this(configuration.getName(), raftProperties.getWaitForLeaderTimeout());
    }

    /**
     * @param node                 the name of the current node
     * @param waitForLeaderTimeout timeout to keep checking when there is no leader for a context
     */
    ContextLeaderProviderImpl(String node, int waitForLeaderTimeout) {
        this.node = node;
        this.waitForLeaderTimeout = waitForLeaderTimeout;
    }

    /**
     * Listens for {@link io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.ContextLeaderConfirmation} events,
     * signalling that there has been a change in the leader for a context.
     * @param masterConfirmation the event
     */
    @EventListener
    public void on(ClusterEvents.ContextLeaderConfirmation masterConfirmation) {
        if (masterConfirmation.node() == null) {
            leaderMap.remove(masterConfirmation.context());
        } else {
            leaderMap.put(masterConfirmation.context(), masterConfirmation.node());
        }
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        leaderMap.remove(contextDeleted.context());
    }

    @EventListener
    public void on(ContextEvents.ContextPreDelete contextDeleted) {
        leaderMap.remove(contextDeleted.context());
    }

    @EventListener
    public void on(ClusterEvents.ContextLeaderStepDown leaderStepDown) {
        leaderMap.remove(leaderStepDown.context());
    }

    @EventListener
    @Order(20)
    public void on(ClusterEvents.BecomeContextLeader becomeLeader) {
        leaderMap.put(becomeLeader.context(), node);
    }

    @Override
    public String getLeader(String context) {
        return leaderMap.get(context);
    }

    @Override
    public boolean isLeader(String context) {
        String leader = leaderMap.get(context);
        return leader != null && leader.equals(node);
    }

    /**
     * Retrieves the current leader for specified {@code context}. If no leader is found it polls a specified amount of
     * time
     * to check if there is a leader. Returns null if no leader could be found within the timeout period.
     *
     * @param context       the context to find the leader for
     * @param waitForLeader wait for leader if no leader found
     * @return the node name of the leader or null
     */
    public String getLeaderOrWait(String context, boolean waitForLeader) {
        String leader = getLeader(context);
        if (leader != null || !waitForLeader) {
            return leader;
        }

        long timeout = System.currentTimeMillis() + waitForLeaderTimeout;
        while (leader == null && System.currentTimeMillis() < timeout) {
            try {
                Thread.sleep(10);
                leader = getLeader(context);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return getLeader(context);
    }
}
