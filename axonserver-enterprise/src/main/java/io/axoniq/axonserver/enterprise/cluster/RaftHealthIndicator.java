package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.util.StringUtils;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class RaftHealthIndicator extends AbstractHealthIndicator {
    private final GrpcRaftController raftController;
    private final RaftLeaderProvider raftLeaderProvider;

    public RaftHealthIndicator(GrpcRaftController raftController,
                               RaftLeaderProvider raftLeaderProvider) {
        this.raftController = raftController;
        this.raftLeaderProvider = raftLeaderProvider;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder)  {
        builder.up();
        raftController.getContexts().forEach(c -> {
            builder.withDetail(c + ".leader", StringUtils.getOrDefault(raftLeaderProvider.getLeader(c), "None"));
        });

    }
}
