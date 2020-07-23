package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.grpc.PlatformService;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

/**
 * Controller to manage client connections when the current node is removed from replication group or context is
 * deleted.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Controller
public class PrepareDeleteNodeController {

    private final PlatformService platformService;
    private final String nodeName;
    private final ReplicationGroupController replicationGroupController;

    public PrepareDeleteNodeController(MessagingPlatformConfiguration configuration,
                                       PlatformService platformService,
                                       ReplicationGroupController replicationGroupController) {
        this.platformService = platformService;
        this.nodeName = configuration.getName();
        this.replicationGroupController = replicationGroupController;
    }

    @EventListener
    public void on(ClusterEvents.DeleteNodeFromReplicationGroupRequested prepareDeleteNodeFromContext) {
        if (nodeName.equals(prepareDeleteNodeFromContext.node())) {
            replicationGroupController.getContextNames(prepareDeleteNodeFromContext.replicationGroup())
                                      .forEach(platformService::requestReconnectForContext);
        }
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted event) {
        platformService.requestReconnectForContext(event.context());
    }
}
