package io.axoniq.axonserver.websocket;

import io.axoniq.axonserver.UserSynchronizationEvents;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
@Service
public class WebsocketUserContext {

    private final SimpMessagingTemplate websocket;

    public WebsocketUserContext(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(UserSynchronizationEvents.UserReceived event) {
        websocket.convertAndSend("/topic/user", event.getClass().getName());
    }
}
