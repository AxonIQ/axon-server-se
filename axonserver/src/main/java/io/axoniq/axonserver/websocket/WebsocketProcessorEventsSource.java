package io.axoniq.axonserver.websocket;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
@Service
public class WebsocketProcessorEventsSource {

    private final SimpMessagingTemplate websocket;

    public WebsocketProcessorEventsSource(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(EventProcessorStatusUpdate event) {
        websocket.convertAndSend("/topic/processor", event.getClass().getName());
    }
}
