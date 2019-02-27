package io.axoniq.axonserver.enterprise.logging;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
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
    public void on(ClusterEvents.LeaderConfirmation event) {
        logger.info("{}: Leader is {}", event.getContext(), event.getNode());
    }

    @EventListener
    public void on(ClusterEvents.BecomeLeader event) {
        logger.info("{}: Leader", event.getContext());
    }

    @EventListener
    public void on(ClusterEvents.LeaderStepDown event) {
        logger.info("{}: No longer leader", event.getContextName());
    }


}
