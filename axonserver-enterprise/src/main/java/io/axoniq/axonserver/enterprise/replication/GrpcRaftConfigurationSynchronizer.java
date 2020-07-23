package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.SameNodesPredicate;
import io.axoniq.axonserver.enterprise.replication.group.RaftGroupService;
import io.axoniq.axonserver.enterprise.replication.group.RaftGroupServiceFactory;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Performs the scheduled operations needed to update the status of pending configuration changes in raft group members.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
@Controller
public class GrpcRaftConfigurationSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(GrpcRaftConfigurationSynchronizer.class);
    private final SameNodesPredicate sameNodes = new SameNodesPredicate();
    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final ReplicationGroupConfigurations replicationGroupConfigurations;
    private final RaftLeaderProvider leaderProvider;
    private final MessagingPlatformConfiguration configuration;

    public GrpcRaftConfigurationSynchronizer(
            RaftGroupServiceFactory raftGroupServiceFactory,
            ReplicationGroupConfigurations replicationGroupConfigurations,
            RaftLeaderProvider leaderProvider,
            MessagingPlatformConfiguration configuration) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.replicationGroupConfigurations = replicationGroupConfigurations;
        this.leaderProvider = leaderProvider;
        this.configuration = configuration;
    }

    @Scheduled(fixedRateString = "${axoniq.axonserver.enterprise.context-configuration-sync-rate:3600000}")
    public void check() {
        try {
            String leader = leaderProvider.getLeader(getAdmin());
            if (leader == null || !leader.equals(me())){
                return;
            }
            replicationGroupConfigurations.forEach(this::verify);
        } catch (Exception e) {
            logger.warn("Exception during contexts configuration sync.", e);
        }
    }

    String me() {
        return configuration.getName();
    }


    private void verify(ReplicationGroupConfiguration configuration) {
        if (configuration.getPending() && isConfirmed(configuration)) {
            ReplicationGroupConfiguration confirmed = ReplicationGroupConfiguration.newBuilder(configuration)
                                                                                   .setPending(false)
                                                                                   .build();
            raftGroupServiceFactory.getRaftGroupService(getAdmin())
                                   .appendEntry(getAdmin(), ReplicationGroupConfiguration.class.getName(),
                                                confirmed.toByteArray());
        }

        if (!configuration.getPending() && !isConfirmed(configuration)) {
            logger.warn("{}: The configuration is not synchronized with the raft status.",
                        configuration.getReplicationGroupName());
        }
    }


    private boolean isConfirmed(ReplicationGroupConfiguration configuration) {
        String context = configuration.getReplicationGroupName();
        try {
            RaftGroupService leader = raftGroupServiceFactory.getRaftGroupService(context);
            ReplicationGroupConfiguration c = leader.configuration(context).get();
            return c != null && !c.getPending() && sameNodes.test(configuration, c);
        } catch (Exception e) {
            logger.warn("Impossible to get the current configuration from the leader of the context {}: {}",
                        context,
                        e.getMessage());
            return false;
        }
    }
}

