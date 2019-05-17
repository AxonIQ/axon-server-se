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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.ConcurrentHashMap.newKeySet;

/**
 * Retrieves leader information about contexts where this node is not a member of. Admin nodes need this information to send
 * admin requests to the right node.
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
    @Scheduled(fixedRate = 5000)
    public void updateStatus() {
        Map<String, Set<String>> leadersPerContext = new ConcurrentHashMap<>();
        updateLeadership(leadersPerContext);
        checkLeadershipChanges(leadersPerContext);
    }

    private void checkLeadershipChanges(
            Map<String, Set<String>> leadersPerContext) {
        Collection<String> myContexts = contextController.getMyContextNames();
        // Ignore information for contexts that I am a member of as leader information for these contexts
        // is updated through the raft groups
//        leadersPerContext.entrySet().removeIf(e -> myContexts.contains(e.getKey()));
        leadersPerContext.forEach((context, leaders) -> {
            String lastKnownLeader = raftLeaderProvider.getLeader(context);
            logger.debug("{}: lastKnown {}, leaders: {}", context, lastKnownLeader, leaders);
            if (myContexts.contains(context)) {
                // On registration of a node there is no event from raft with the leader
                // TODO: MGA: find a better way for this
                if (lastKnownLeader == null && !leaders.isEmpty()) {
                    String anyLeader = leaders.stream()
                                              .findAny()
                                              .get();
                    publishLeaderConfirmation(context, anyLeader);
                }
            } else {
                if (leaders.isEmpty()) {
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
            }
        });
    }

    private void updateLeadership(Map<String, Set<String>> leadersPerContext) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        contextController.getRemoteNodes()
                         .forEach(node -> futures.add(raftServiceFactory.getRaftGroupServiceForNode(node)
                                                            .getStatus(context -> updateLeader(context, leadersPerContext))));

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.info("Exception while waiting for status from remote nodes", e);
        } catch (TimeoutException e) {
            // Ignore timeout
            logger.info("Timeout while waiting for status from remote nodes");
        }
    }

    private void updateLeader(Context context, Map<String, Set<String>> leadersPerContext) {
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
