package io.axoniq.axonhub.websocket;

import io.axoniq.axonhub.ClusterEvents;
import io.axoniq.axonhub.SubscriptionEvents;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Author: marc
 */
@Service
public class ClusterUpdatesListener {
    private final SimpMessagingTemplate websocket;

    public ClusterUpdatesListener(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(ClusterEvents.ClusterBaseEvent clusterEvent) {
        websocket.convertAndSend("/topic/cluster", clusterEvent.getClass().getName());
    }

    @EventListener
    public void on(SubscriptionEvents.SubscriptionBaseEvent subscriptionEvent) {
        websocket.convertAndSend("/topic/subscriptions", subscriptionEvent.getClass().getName());
    }
}
