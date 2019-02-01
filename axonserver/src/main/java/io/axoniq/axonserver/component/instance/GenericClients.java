package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
@Primary @Component
public class GenericClients implements Clients {

    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final Map<ClientIdentification, Client> clientRegistrations = new HashMap<>();

    public GenericClients(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public Iterator<Client> iterator() {
        return clientRegistrations.values().iterator();
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        this.clientRegistrations.remove(event.clientIdentification());
    }

    @EventListener
    public void on(TopologyEvents.ApplicationConnected event) {
        this.clientRegistrations.put(event.clientIdentification(),
                                     new GenericClient(event.getClient(),
                                                       event.getComponentName(),
                                                       event.getContext(),
                                                       event.isProxied() ? event.getProxy() : messagingPlatformConfiguration
                                                               .getName()));
    }
}
