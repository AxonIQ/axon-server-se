package io.axoniq.axonserver.websocket;

import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
@Service
public class WebsocketProcessorContext {

    private final SimpMessagingTemplate websocket;

    public WebsocketProcessorContext(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

}
