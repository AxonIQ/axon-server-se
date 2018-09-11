package io.axoniq.axonserver.websocket;

import io.axoniq.axonserver.enterprise.cluster.events.ApplicationSynchronizationEvents;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
@Service
public class WebsocketApplicationContext {

    private final SimpMessagingTemplate websocket;

    public WebsocketApplicationContext(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(ApplicationSynchronizationEvents.ApplicationReceived event) {
        websocket.convertAndSend("/topic/application", event.getClass().getName());
    }
}
