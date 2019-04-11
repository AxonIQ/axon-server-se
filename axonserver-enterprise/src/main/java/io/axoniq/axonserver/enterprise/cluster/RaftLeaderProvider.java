package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import org.springframework.context.event.EventListener;
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

    public RaftLeaderProvider(MessagingPlatformConfiguration configuration) {
        this.node = configuration.getName();
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
    public void on(ClusterEvents.LeaderStepDown leaderStepDown) {
        leaderMap.remove(leaderStepDown.getContextName());
    }

    @EventListener
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
}
