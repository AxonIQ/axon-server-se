package io.axoniq.axonserver.websocket;

import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.TopologyEvents;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * @author Marc Gathier
 */
@Service
public class ClusterUpdatesListener {
    private final SimpMessagingTemplate websocket;

    public ClusterUpdatesListener(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(TopologyEvents.TopologyBaseEvent clusterEvent) {
        websocket.convertAndSend("/topic/cluster", clusterEvent.getClass().getName());
    }

    @EventListener
    public void on(SubscriptionEvents.SubscriptionBaseEvent subscriptionEvent) {
        websocket.convertAndSend("/topic/subscriptions", subscriptionEvent.getClass().getName());
    }
}
