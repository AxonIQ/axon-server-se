package io.axoniq.axonhub.logging;

import io.axoniq.axonhub.ClusterEvents;
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
    public void on(ClusterEvents.ApplicationConnected event) {
        if( event.isProxied()) {
            logger.info("Application connected via {}: {}, clientId = {}, context = {}", event.getProxy(), event.getComponentName(), event.getClient(), event.getContext());
        } else {
            logger.info("Application connected: {}, clientId = {}, context = {}", event.getComponentName(), event.getClient(), event.getContext());
        }
    }

    @EventListener
    public void on(ClusterEvents.ApplicationDisconnected event) {
        if( event.isProxied()) {
            logger.info("Application disconnected via {}: {}, clientId = {}, context = {}", event.getProxy(), event.getComponentName(), event.getClient(), event.getContext());
        } else {
            logger.info("Application disconnected: {}, clientId = {}, context = {}", event.getComponentName(), event.getClient(), event.getContext());
        }
    }


}
