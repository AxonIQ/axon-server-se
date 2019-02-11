package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

/**
 * @author Marc Gathier
 */
@Controller
public class GrpcRaftStatusManager {
    private final Logger logger = LoggerFactory.getLogger(GrpcRaftStatusManager.class);
    private final ContextController contextController;
    private final RaftGroupServiceFactory raftServiceFactory;
    private final RaftLeaderProvider raftLeaderProvider;
    private final ApplicationEventPublisher eventPublisher;

    public GrpcRaftStatusManager(ContextController contextController,
                                 RaftGroupServiceFactory raftServiceFactory,
                                 RaftLeaderProvider raftLeaderProvider,
                                 ApplicationEventPublisher eventPublisher) {
        this.contextController = contextController;
        this.raftServiceFactory = raftServiceFactory;
        this.raftLeaderProvider = raftLeaderProvider;
        this.eventPublisher = eventPublisher;
    }

    /**
     * At a fixed delay, request a status update from all nodes. Status includes what nodes are leaders within which context.
     * Sent to all nodes, each node will reply for its contexts
     */
    @Scheduled(fixedDelay = 5000)
    public void updateStatus() {
        contextController.getNodes().forEach(node -> raftServiceFactory.getRaftGroupServiceForNode(node)
                                                                       .getStatus(this::updateLeader));
    }

    private void updateLeader(Context context) {
        context.getMembersList().forEach(cm -> {
            if( State.LEADER.getNumber() == cm.getState().getNumber() && ! cm.getNodeName().equals(raftLeaderProvider.getLeader(context.getName())) ) {
                    logger.warn("{}: new leader id {}, name {}", context.getName(), cm.getNodeId(), cm.getNodeName());
                    eventPublisher.publishEvent(new ClusterEvents.LeaderConfirmation(context.getName(), cm.getNodeName(), true));
            }
        });
    }
}
