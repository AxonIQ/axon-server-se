package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class ClientContextListener {

    private final PlatformService platformService;
    private final String nodeName;


    public ClientContextListener(PlatformService platformService, Topology topology) {
        this.platformService = platformService;
        this.nodeName = topology.getName();
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        platformService.requestReconnectForContext(contextDeleted.getName());
    }

    @EventListener
    public void on(ContextEvents.NodeDeletedFromContext nodeDeletedFromContext) {
        if( nodeName.equals(nodeDeletedFromContext.getNode())) {
            platformService.requestReconnectForContext(nodeDeletedFromContext.getName());
        }
    }

}
