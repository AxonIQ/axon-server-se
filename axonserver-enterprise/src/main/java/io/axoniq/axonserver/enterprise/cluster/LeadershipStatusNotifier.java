package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.ConcurrentHashMap.newKeySet;

/**
 * Informs the world about leadership changes in the cluster.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class LeadershipStatusNotifier {

    private static final Logger logger = LoggerFactory.getLogger(LeadershipStatusNotifier.class);

    private final ContextController contextController;
    private final RaftGroupServiceFactory raftServiceFactory;
    private final RaftLeaderProvider raftLeaderProvider;
    private final ApplicationEventPublisher eventPublisher;
    private final Map<String, Set<String>> leadersPerContext = new ConcurrentHashMap<>();

    /**
     * Creates an instance of Leadership Status Notifier.
     *
     * @param contextController  used for getting information about contexts
     * @param raftServiceFactory used for instantiating {@link RaftGroupService}
     * @param raftLeaderProvider provides the information about the last known leader for given context
     * @param eventPublisher     publishes leadership confirmation events
     */
    public LeadershipStatusNotifier(ContextController contextController,
                                    RaftGroupServiceFactory raftServiceFactory,
                                    RaftLeaderProvider raftLeaderProvider,
                                    ApplicationEventPublisher eventPublisher) {
        this.contextController = contextController;
        this.raftServiceFactory = raftServiceFactory;
        this.raftLeaderProvider = raftLeaderProvider;
        this.eventPublisher = eventPublisher;
    }

    /**
     * At a fixed delay, request a status update from all nodes. Status includes what nodes are leaders within which
     * context.
     * Sent to all nodes, each node will reply for its contexts.
     */
    @Scheduled(fixedDelay = 5000)
    public void updateStatus() {
        checkLeadershipChanges();
        leadersPerContext.clear();
        updateLeadership();
    }

    private void checkLeadershipChanges() {
        leadersPerContext.forEach((context, leaders) -> {
            String lastKnownLeader = raftLeaderProvider.getLeader(context);
            if (leaders.size() == 0) {
                if (lastKnownLeader != null) {
                    publishLeaderConfirmation(context, null);
                }
            } else {
                if (leaders.size() > 1) {
                    logger.warn("Found {} leaders for context {}. They are {}.",
                                leaders.size(),
                                context,
                                String.join(",", leaders));
                }
                if (lastKnownLeader == null || !leaders.contains(lastKnownLeader)) {
                    String anyLeader = leaders.stream()
                                              .findAny()
                                              .get();
                    publishLeaderConfirmation(context, anyLeader);
                }
            }
        });
    }

    private void updateLeadership() {
        contextController.getRemoteNodes()
                         .forEach(node -> raftServiceFactory.getRaftGroupServiceForNode(node)
                                                            .getStatus(this::updateLeader));
    }

    private void updateLeader(Context context) {
        context.getMembersList()
               .stream()
               .filter(cm -> State.LEADER.getNumber() == cm.getState().getNumber())
               .findFirst()
               .ifPresent(leader -> leadersPerContext.computeIfAbsent(context.getName(), n -> newKeySet())
                                                     .add(leader.getNodeName()));
    }

    private void publishLeaderConfirmation(String context, String leader) {
        eventPublisher.publishEvent(new ClusterEvents.LeaderConfirmation(context, leader, true));
    }
}
