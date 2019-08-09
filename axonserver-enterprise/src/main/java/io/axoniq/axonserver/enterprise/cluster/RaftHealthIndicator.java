package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

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
            RaftNode thisNode = raftController.getRaftNode(c);
            if (thisNode.isLeader()) {
                RaftGroup raftGroup = raftController.getRaftGroup(c);
                long lastLogIndex = raftGroup.localLogEntryStore().lastLogIndex();
                long now = System.currentTimeMillis();
                thisNode.replicatorPeers().forEachRemaining(
                        rp -> {
                            long lastMessageAge = now - rp.lastMessageReceived();
                            long raftMsgBuffer = lastLogIndex - (rp.nextIndex() - 1);
                            if (lastMessageAge > raftGroup.raftConfiguration().maxElectionTimeout()) {
                                builder.withDetail(c + ".follower." + rp.nodeName() + ".status", "NO_ACK_RECEIVED");
                                builder.status(new Status("WARN"));
                            } else if(raftMsgBuffer > 100) {
                                builder.withDetail(c + ".follower." + rp.nodeName() + ".status", "BEHIND");
                                builder.status(new Status("WARN"));
                            } else {
                                builder.withDetail(c + ".follower." + rp.nodeName() + ".status", "UP");
                            }
                        }
                );
            }
            String leader = thisNode.getLeaderName();
            if (leader == null) {
                builder.status(new Status("WARN"));
            } else {
                hasAnyLeader.set(true);
            }
            builder.withDetail(c + ".leader", StringUtils.getOrDefault(leader, "None"));
        });
        if (!hasAnyLeader.get() && raftController.getContexts().size() > 0) {
            builder.down();
        }
    }
}
