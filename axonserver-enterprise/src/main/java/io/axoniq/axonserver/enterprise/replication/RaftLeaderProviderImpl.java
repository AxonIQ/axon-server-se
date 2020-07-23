package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class RaftLeaderProviderImpl implements RaftLeaderProvider {

    private final Logger logger = LoggerFactory.getLogger(RaftLeaderProviderImpl.class);
    private final Map<String, String> leaderMap = new ConcurrentHashMap<>();

    private final String node;
    private final Function<String, Set<String>> contextsPerReplicationGroup;
    private final ApplicationEventPublisher applicationEventPublisher;

    @Autowired
    public RaftLeaderProviderImpl(MessagingPlatformConfiguration configuration,
                                  RaftGroupRepositoryManager raftGroupRepositoryManager,
                                  AdminReplicationGroupController adminContextController,
                                  ApplicationEventPublisher applicationEventPublisher) {
        this(configuration.getName(),
             r -> {
                 Set<String> local = raftGroupRepositoryManager.contextsPerReplicationGroup(r);
                 local.addAll(adminContextController.contextsPerReplicationGroup(r));
                 return local;
             },
             applicationEventPublisher);
    }

    RaftLeaderProviderImpl(String node, Function<String, Set<String>> contextsPerReplicationGroup,
                           ApplicationEventPublisher applicationEventPublisher) {
        this.node = node;
        this.contextsPerReplicationGroup = contextsPerReplicationGroup;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @EventListener
    public void on(ClusterEvents.ReplicationGroupUpdated event) {
        if (leaderMap.containsKey(event.replicationGroup())) {
            contextsPerReplicationGroup.apply(event.replicationGroup())
                                       .forEach(context -> applicationEventPublisher.publishEvent(
                                               new ClusterEvents.ContextLeaderConfirmation(context,
                                                                                           leaderMap
                                                                                                   .get(event.replicationGroup()))));
        }
    }


    @EventListener
    public void on(ClusterEvents.LeaderConfirmation masterConfirmation) {
        if (masterConfirmation.node() == null) {
            leaderMap.remove(masterConfirmation.replicationGroup());
        } else {
            leaderMap.put(masterConfirmation.replicationGroup(), masterConfirmation.node());
        }

        contextsPerReplicationGroup.apply(masterConfirmation.replicationGroup())
                                   .forEach(context -> applicationEventPublisher.publishEvent(
                                           new ClusterEvents.ContextLeaderConfirmation(context,
                                                                                       masterConfirmation.node())));
    }

    @EventListener
    public void on(ClusterEvents.LeaderNotification leaderNotification) {
        if (!leaderNotification.node().equals(leaderMap.get(leaderNotification.replicationGroup()))) {
            leaderMap.put(leaderNotification.replicationGroup(), leaderNotification.node());
            contextsPerReplicationGroup.apply(leaderNotification.replicationGroup())
                                       .forEach(context -> applicationEventPublisher.publishEvent(
                                               new ClusterEvents.ContextLeaderConfirmation(context,
                                                                                           leaderNotification.node())));
        }
    }

    @EventListener
    public void on(ClusterEvents.LeaderStepDown leaderStepDown) {
        leaderMap.remove(leaderStepDown.replicationGroup());
        contextsPerReplicationGroup.apply(leaderStepDown.replicationGroup())
                                   .forEach(context -> applicationEventPublisher.publishEvent(
                                           new ClusterEvents.ContextLeaderStepDown(context)));
    }

    @EventListener
    @Order(10)
    public void on(ClusterEvents.BecomeLeader becomeLeader) {
        leaderMap.put(becomeLeader.replicationGroup(), node);
        contextsPerReplicationGroup.apply(becomeLeader.replicationGroup())
                                   .forEach(context -> applicationEventPublisher.publishEvent(
                                           new ClusterEvents.BecomeContextLeader(context,
                                                                                 becomeLeader
                                                                                         .unappliedEntriesSupplier())));
        applicationEventPublisher.publishEvent(new ClusterEvents.LeaderNotification(becomeLeader.replicationGroup(),
                                                                                    node));
    }

    @Override
    public String getLeader(String replicationGroup) {
        return leaderMap.get(replicationGroup);
    }

    @Override
    public boolean isLeader(String context) {
        String leader = leaderMap.get(context);
        logger.debug("{}: checking leader {}", context, leader);
        return leader != null && leader.equals(node);
    }

    @Override
    public Set<String> leaderFor() {
        return leaderMap.keySet()
                        .stream()
                        .filter(this::isLeader)
                        .collect(Collectors.toSet());
    }

}
