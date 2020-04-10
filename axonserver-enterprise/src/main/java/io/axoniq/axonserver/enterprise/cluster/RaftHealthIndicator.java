package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.axoniq.axonserver.enterprise.HealthStatus.WARN_STATUS;

/**
 * @author Marc Gathier
 */
@Component
public class RaftHealthIndicator extends AbstractHealthIndicator {
    private final GrpcRaftController raftController;

    public RaftHealthIndicator(GrpcRaftController raftController) {
        this.raftController = raftController;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();
        AtomicBoolean hasAnyLeader = new AtomicBoolean();
        raftController.getContexts().forEach(c -> {
            try {
                RaftNode thisNode = raftController.getRaftNode(c);
                if (!thisNode.health(builder::withDetail)) {
                    builder.status(WARN_STATUS);
                }

                String leader = thisNode.getLeaderName();
                if (leader == null) {
                    builder.status(WARN_STATUS);
                } else {
                    hasAnyLeader.set(true);
                }
                builder.withDetail(c + ".leader", StringUtils.getOrDefault(leader, "None"));

                if (thisNode.unappliedEntriesCount() > 100) {
                    builder.withDetail(c + ".status", "BEHIND");
                    builder.status(WARN_STATUS);
                }
            } catch (IllegalStateException ex) {
                builder.withDetail(c + ".status", ex.getMessage());
                builder.status(WARN_STATUS);
            }
        });
        if (!hasAnyLeader.get() && !raftController.getContexts().isEmpty()) {
            builder.down();
        }
    }
}
