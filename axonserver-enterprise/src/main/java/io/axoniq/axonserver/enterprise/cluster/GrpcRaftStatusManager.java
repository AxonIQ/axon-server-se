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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: marc
 */
@Controller
public class GrpcRaftStatusManager {
    private final Logger logger = LoggerFactory.getLogger(GrpcRaftStatusManager.class);
    private final Map<String, String> leaderMap = new ConcurrentHashMap<>();
    private final ContextController contextController;
    private final RaftGroupServiceFactory raftServiceFactory;
    private final ApplicationEventPublisher eventPublisher;

    public GrpcRaftStatusManager(ContextController contextController,
                                 RaftGroupServiceFactory raftServiceFactory,
                                 ApplicationEventPublisher eventPublisher) {
        this.contextController = contextController;
        this.raftServiceFactory = raftServiceFactory;
        this.eventPublisher = eventPublisher;
    }

    @Scheduled(fixedDelay = 5000)
    public void updateStatus() {
        contextController.getNodes().forEach(node -> raftServiceFactory.getRaftGroupServiceForNode(node)
                                                                       .getStatus(this::updateLeader));
    }

    private void updateLeader(Context context) {
        context.getMembersList().forEach(cm -> {
            if( State.LEADER.getNumber() == cm.getState().getNumber()) {
                if( ! cm.getNodeId().equals(leaderMap.get(context.getName())) ) {
                    leaderMap.put(context.getName(), cm.getNodeId());
                    logger.warn("{}: Leader {}", context.getName(), cm.getNodeId());
                    eventPublisher.publishEvent(new ClusterEvents.LeaderConfirmation(context.getName(), cm.getNodeId(), true));
                }
            }
        });
    }
}
