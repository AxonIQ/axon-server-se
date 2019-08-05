package io.axoniq.axonserver.enterprise.cluster;

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
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class RaftLeaderProvider {
    private final Map<String, String> leaderMap = new ConcurrentHashMap<>();

    private final String node;
    private final int waitForLeaderTimeout;

    @Autowired
    public RaftLeaderProvider(MessagingPlatformConfiguration configuration,
                              RaftProperties raftProperties) {
        this(configuration.getName(), raftProperties.getWaitForLeaderTimeout());
    }

    RaftLeaderProvider(String node, int waitForLeaderTimeout) {
        this.node = node;
        this.waitForLeaderTimeout = waitForLeaderTimeout;
    }

    @EventListener
    public void on(ClusterEvents.LeaderConfirmation masterConfirmation) {
        if( masterConfirmation.getNode() == null) {
            leaderMap.remove(masterConfirmation.getContext());
        } else {
            leaderMap.put(masterConfirmation.getContext(), masterConfirmation.getNode());
        }
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        leaderMap.remove(contextDeleted.getContext());
    }

    @EventListener
    public void on(ClusterEvents.LeaderStepDown leaderStepDown) {
        leaderMap.remove(leaderStepDown.getContextName());
    }

    @EventListener
    @Order(10)
    public void on(ClusterEvents.BecomeLeader becomeLeader) {
        leaderMap.put(becomeLeader.getContext(), node);
    }

    public String getLeader(String context) {
        return leaderMap.get(context);
    }
    public boolean isLeader(String context) {
        String leader = leaderMap.get(context);
        return leader != null && leader.equals(node);
    }

    /**
     * Retrieves the current leader for specified {@code context}. If no leader is found it polls a specified amount of
     * time
     * to check if there is a leader. Returns null if no leader could be found within the timeout period.
     *
     * @param context the context to find the leader for
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
