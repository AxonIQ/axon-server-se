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
    public void on(ClusterEvents.AxonHubInstanceConnected event) {
        logger.info("AxonHub instance connected: {}", event.getRemoteConnection().getClusterNode().getName());
    }

    @EventListener
    public void on(ClusterEvents.AxonHubInstanceDisconnected event) {
        logger.info("AxonHub instance disconnected: {}", event.getNodeName());
    }

    @EventListener
    public void on(ClusterEvents.MasterConfirmation event) {
        logger.info("MasterConfirmation: context {}, master = {}", event.getContext(), event.getNode());
    }

    @EventListener
    public void on(ClusterEvents.BecomeMaster event) {
        logger.info("BecomeMaster: context {}, master = {}", event.getContext(), event.getNode());
    }

    @EventListener
    public void on(ClusterEvents.MasterStepDown event) {
        logger.info("MasterStepDown: context {}", event.getContextName());
    }

    @EventListener
    public void on(ClusterEvents.BecomeCoordinator event) {
        logger.info("{} became cluster coordinator for {}", event.node(), event.context());
    }

    @EventListener
    public void on(ClusterEvents.CoordinatorConfirmation event) {
        logger.info("{} became cluster coordinator for {}", event.node(), event.context());
    }

}
