package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftGroup;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

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
    protected void doHealthCheck(Health.Builder builder)  {
        builder.up();
        raftController.getContexts().forEach(c -> {
            RaftGroup raftGroup = raftController.getRaftGroup(c);
            builder.withDetail(c + ".leader", raftGroup.localNode().getLeaderName());
        });

    }
}
