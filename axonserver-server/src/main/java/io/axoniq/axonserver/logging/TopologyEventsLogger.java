package io.axoniq.axonserver.logging;

import io.axoniq.axonserver.TopologyEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class TopologyEventsLogger {
    private final Logger logger = LoggerFactory.getLogger(TopologyEventsLogger.class);

    @EventListener
    public void on(TopologyEvents.ApplicationConnected event) {
        if( event.isProxied()) {
            logger.info("Application connected via {}: {}, clientId = {}, context = {}", event.getProxy(), event.getComponentName(), event.getClient(), event.getContext());
        } else {
            logger.info("Application connected: {}, clientId = {}, context = {}", event.getComponentName(), event.getClient(), event.getContext());
        }
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        if( event.isProxied()) {
            logger.info("Application disconnected via {}: {}, clientId = {}, context = {}", event.getProxy(), event.getComponentName(), event.getClient(), event.getContext());
        } else {
            logger.info("Application disconnected: {}, clientId = {}, context = {}", event.getComponentName(), event.getClient(), event.getContext());
        }
    }


}
