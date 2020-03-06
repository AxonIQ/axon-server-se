package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.SameNodesPredicate;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
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
    private final Iterable<ContextConfiguration> contextConfigurations;
    private final RaftLeaderProvider leaderProvider;
    private final MessagingPlatformConfiguration configuration;

    public GrpcRaftConfigurationSynchronizer(
            RaftGroupServiceFactory raftGroupServiceFactory,
            Iterable<ContextConfiguration> contextConfigurations,
            RaftLeaderProvider leaderProvider,
            MessagingPlatformConfiguration configuration) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.contextConfigurations = contextConfigurations;
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
            contextConfigurations.forEach(this::verify);
        } catch (Exception e) {
            logger.warn("Exception during contexts configuration sync.", e);
        }
    }

    String me() {
        return configuration.getName();
    }


    private void verify(ContextConfiguration configuration) {
        if (configuration.getPending() && isConfirmed(configuration)) {
            ContextConfiguration confirmed = ContextConfiguration.newBuilder(configuration)
                                                                 .setPending(false)
                                                                 .build();
            raftGroupServiceFactory.getRaftGroupService(getAdmin())
                                   .appendEntry(getAdmin(), ContextConfiguration.class.getName(),
                                                confirmed.toByteArray());
        }

        if (!configuration.getPending() && !isConfirmed(configuration)){
            logger.warn("{}: The configuration is not synchronized with the raft status.", configuration.getContext());
        }
    }


    private boolean isConfirmed(ContextConfiguration configuration) {
        String context = configuration.getContext();
        try {
            RaftGroupService leader = raftGroupServiceFactory.getRaftGroupService(context);
            ContextConfiguration c = leader.configuration(context).get();
            return c != null && !c.getPending() && sameNodes.test(configuration, c);
        } catch (Exception e) {
            logger.warn("Impossible to get the current configuration from the leader of the context {}: {}",
                        context,
                        e.getMessage());
            return false;
        }
    }
}

