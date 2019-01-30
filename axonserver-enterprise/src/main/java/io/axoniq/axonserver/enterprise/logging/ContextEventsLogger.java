package io.axoniq.axonserver.enterprise.logging;

import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class ContextEventsLogger {
    private final Logger logger = LoggerFactory.getLogger(ContextEventsLogger.class);

    @EventListener
    public void on(ContextEvents.ContextDeleted event) {
        logger.info("{}: context deleted", event.getName());
    }

    @EventListener
    public void on(ContextEvents.ContextCreated event) {
        logger.info("{}: Context created, nodes: {}", event.getName(), event.getNodes());
    }

    @EventListener
    public void on(ContextEvents.NodeRolesUpdated event) {
        logger.info("{}: Context updated, node: {}", event.getName(), event.getNode().getName());
    }

}
