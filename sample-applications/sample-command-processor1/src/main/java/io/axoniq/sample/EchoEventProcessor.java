package io.axoniq.sample;

import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.stereotype.Component;

/**
 * Created by Sara Pellegrini on 09/03/2018.
 * sara.pellegrini@gmail.com
 */
@Component
@ProcessingGroup("sample")
public class EchoEventProcessor {

    @EventHandler
    public void on(EchoEvent echoEvent){
        System.out.println(echoEvent.getId());
    }

}
