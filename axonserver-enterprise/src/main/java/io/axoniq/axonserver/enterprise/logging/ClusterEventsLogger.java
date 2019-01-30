package io.axoniq.axonserver.enterprise.logging;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class ClusterEventsLogger {
    private final Logger logger = LoggerFactory.getLogger(ClusterEventsLogger.class);

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected event) {
        logger.info("AxonServer node connected: {}", event.getRemoteConnection().getClusterNode().getName());
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceDisconnected event) {
        logger.info("AxonServer node disconnected: {}", event.getNodeName());
    }

    @EventListener
    public void on(ClusterEvents.MasterConfirmation event) {
        logger.info("{}: master = {}", event.getContext(), event.getNode());
    }

    @EventListener
    public void on(ClusterEvents.BecomeMaster event) {
        logger.info("{}: master = {}", event.getContext(), event.getNode());
    }

    @EventListener
    public void on(ClusterEvents.MasterStepDown event) {
        logger.info("{}: master step down", event.getContextName());
    }

    @EventListener
    public void on(ClusterEvents.BecomeCoordinator event) {
        logger.info("{}: cluster coordinator = {}", event.context(), event.node());
    }

    @EventListener
    public void on(ClusterEvents.CoordinatorConfirmation event) {
        logger.info("{}: cluster coordinator = {}", event.context(), event.node());
    }

}
